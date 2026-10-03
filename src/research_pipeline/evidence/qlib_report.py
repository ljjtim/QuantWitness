"""从已验证 Result 生成有界的 Qlib 模型研究报告。"""

from __future__ import annotations

import html
import json
from pathlib import Path
from typing import Mapping

import pyarrow as pa
import pyarrow.compute as pc

REPORT_REQUEST_VERSION = "qlib-research-report-v1"
_GRAPHS = {"group_return", "pred_ic", "pred_autocorr"}
_COLUMNS = {"instrument": "entity_id", "datetime": "observation_session", "score": "prediction", "label": "actual"}


def load_report_request(path: str | Path) -> dict:
    """JSON 和 YAML 使用同一声明合同。"""
    import yaml

    value = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return validate_report_request(value)


def validate_report_request(value: object) -> dict:
    if not isinstance(value, Mapping):
        raise ValueError("Qlib 报告请求必须为对象")
    from .portfolio_report import PORTFOLIO_REPORT_VERSION, validate_portfolio_request

    if value.get("contract_version") == PORTFOLIO_REPORT_VERSION:
        return validate_portfolio_request(value)
    request = dict(value)
    fields = {"contract_version", "result_id", "table_id", "selection", "window", "columns", "method", "budget"}
    if set(request) != fields or request["contract_version"] != REPORT_REQUEST_VERSION:
        raise ValueError("Qlib 报告请求字段或 contract_version 无效")
    for name in ("result_id", "table_id"):
        if not isinstance(request[name], str) or not request[name]:
            raise ValueError(f"Qlib 报告 {name} 必须为非空字符串")
    for name in ("selection", "window", "columns", "method", "budget"):
        if not isinstance(request[name], Mapping):
            raise ValueError(f"Qlib 报告 {name} 必须为对象")
        request[name] = dict(request[name])
    selection = request["selection"]
    if set(selection) != {"candidate_id", "stage", "fold_id", "horizon_sessions"}:
        raise ValueError("selection 必须明确 candidate_id/stage/fold_id/horizon_sessions")
    for name in ("candidate_id", "stage", "fold_id"):
        if not isinstance(selection[name], str) or not selection[name]:
            raise ValueError(f"selection.{name} 必须为非空字符串")
    if type(selection["horizon_sessions"]) is not int or selection["horizon_sessions"] < 1:
        raise ValueError("horizon_sessions 必须为正整数")
    # 跨 fold 仅接受 selection 生成的逐时点唯一预测；重复行仍拒绝。
    if selection["fold_id"] == "*" and selection["stage"] != "test":
        raise ValueError("跨 fold 报告只支持 selection 节点产生的 stage=test 滚动输出")
    if set(request["window"]) != {"start", "end"}:
        raise ValueError("window 必须给 start/end")
    from datetime import date

    try:
        start, end = (date.fromisoformat(str(request["window"][key])) for key in ("start", "end"))
    except ValueError as exc:
        raise ValueError("报告窗口必须为 ISO 日期") from exc
    if start > end:
        raise ValueError("报告窗口 start 晚于 end")
    request["window"] = {"start": start.isoformat(), "end": end.isoformat()}
    if request["columns"] != _COLUMNS:
        raise ValueError("首批报告列必须映射 entity_id/observation_session/prediction/actual；label 使用原始收益")
    method = request["method"]
    if set(method) - {"graphs", "groups", "ic_methods", "lag", "reverse"}:
        raise ValueError("Qlib 报告 method 包含未知字段")
    method.setdefault("graphs", ["group_return", "pred_ic", "pred_autocorr"])
    method.setdefault("groups", 5)
    method.setdefault("ic_methods", ["IC", "Rank IC"])
    method.setdefault("lag", 1)
    method.setdefault("reverse", False)
    if (not isinstance(method["graphs"], list) or not method["graphs"]
            or any(item not in _GRAPHS for item in method["graphs"])
            or len(set(method["graphs"])) != len(method["graphs"])):
        raise ValueError("graphs 只支持 group_return/pred_ic/pred_autocorr 且不可重复")
    if (type(method["groups"]) is not int or method["groups"] < 2
            or type(method["lag"]) is not int or method["lag"] < 1
            or type(method["reverse"]) is not bool):
        raise ValueError("groups/lag/reverse 无效")
    if method["ic_methods"] != ["IC", "Rank IC"]:
        raise ValueError("首批报告显式计算 IC 和 Rank IC")
    if set(request["budget"]) != {"max_rows", "memory_bytes"} or any(
        type(number) is not int or number < 1 for number in request["budget"].values()
    ):
        raise ValueError("报告预算必须给正整数 max_rows/memory_bytes")
    return request


def read_prediction_frame(context, request: Mapping):
    """按列流式筛选；在 pandas 与图形计算前约束实际选中数据。"""
    import pandas as pd
    from datetime import date

    if context.snapshot.bundle.result_id != request["result_id"]:
        raise ValueError("报告请求 result_id 与 VerificationResult 关联结果不同")
    tables = [table for table in context.snapshot.bundle.tables if table.table_id == request["table_id"]]
    if len(tables) != 1:
        raise ValueError("报告 table_id 不存在或不唯一")
    schema_id = tables[0].schema_id
    columns = tuple(dict.fromkeys((*_COLUMNS.values(), "candidate_id", "stage", "fold_id", "horizon_sessions", "score_semantics")))
    schema = context.snapshot.table_schema(schema_id)
    missing = set(columns) - set(schema.names)
    if missing:
        raise ValueError(f"报告预测表缺少字段: {sorted(missing)}")
    if not pa.types.is_date32(schema.field("observation_session").type):
        raise ValueError("observation_session 必须为日频 Arrow date32")
    budget = request["budget"]
    selection = request["selection"]
    dates = {key: date.fromisoformat(value) for key, value in request["window"].items()}
    batches, rows, used = [], 0, 0
    batch_size = max(1, min(8192, budget["max_rows"] + 1, budget["memory_bytes"] // 1024))
    for batch in context.snapshot.iter_table_batches(schema_id, columns=columns, batch_size=batch_size):
        if batch.nbytes + used * 8 > budget["memory_bytes"]:
            raise ValueError("报告读取超出 memory_bytes，请缩小窗口或增加预算")
        mask = pc.and_(pc.greater_equal(batch.column("observation_session"), dates["start"]),
                       pc.less_equal(batch.column("observation_session"), dates["end"]))
        for name, value in selection.items():
            if name == "fold_id" and value == "*":
                continue
            mask = pc.and_(mask, pc.equal(batch.column(name), value))
        selected = batch.filter(mask)
        rows += selected.num_rows
        used += selected.nbytes
        if rows > budget["max_rows"] or used * 8 > budget["memory_bytes"]:
            raise ValueError("报告样本超出 max_rows/memory_bytes，请缩小窗口；不会自动抽样")
        if selected.num_rows:
            batches.append(selected)
    if not batches:
        raise ValueError("报告筛选没有预测样本")
    frame = pa.Table.from_batches(batches).to_pandas()
    if int(frame.memory_usage(deep=True).sum()) * 3 > budget["memory_bytes"]:
        raise ValueError("报告 pandas 数据超出 memory_bytes，请缩小窗口")
    if frame["entity_id"].isna().any() or frame["observation_session"].isna().any():
        raise ValueError("报告证券或日期为空")
    duplicates = frame.duplicated(["entity_id", "observation_session"], keep=False)
    if duplicates.any():
        sources = frame.loc[duplicates, ["candidate_id", "fold_id", "stage"]].drop_duplicates().to_dict("records")
        raise ValueError(f"报告证券日期重复，存在重叠来源: {sources}")
    semantics = set(frame["score_semantics"].dropna())
    if len(semantics) != 1 or not semantics <= {"raw_return_prediction", "ranking_score"}:
        raise ValueError("报告必须使用唯一已声明的 score_semantics")
    frame = frame.rename(columns={source: target for target, source in _COLUMNS.items()})
    frame["datetime"] = pd.to_datetime(frame["datetime"])
    result = frame.set_index(["instrument", "datetime"])[["score", "label"]].sort_index()
    result.attrs["score_semantics"] = next(iter(semantics))
    return result


def _model_figures(frame, method):
    import numpy as np
    import pandas as pd
    from qlib.contrib.report.analysis_model.analysis_model_performance import model_performance_graph, ic_figure
    from qlib.contrib.report.graph import ScatterGraph

    frame = frame.replace([np.inf, -np.inf], np.nan)
    complete = frame.dropna(subset=["score", "label"])
    by_day = complete.groupby(level="datetime")
    counts = by_day.size()
    score_unique = by_day["score"].nunique()
    label_unique = by_day["label"].nunique()
    diagnostics = {
        "rows": len(frame), "complete_rows": len(complete),
        "missing_score_rows": int(frame["score"].isna().sum()),
        "missing_label_rows": int(frame["label"].isna().sum()),
        "sessions": int(frame.index.get_level_values("datetime").nunique()),
        "instruments": int(frame.index.get_level_values("instrument").nunique()),
        "score_semantics": frame.attrs["score_semantics"],
        "mse": None if complete.empty or frame.attrs["score_semantics"] != "raw_return_prediction" else float(((complete["score"] - complete["label"]) ** 2).mean()),
    }
    figures, notes = [], []
    kwargs = dict(lag=method["lag"], N=method["groups"], reverse=method["reverse"],
                  methods=tuple(method["ic_methods"]), show_notebook=False)
    if "group_return" in method["graphs"]:
        eligible = counts.index[(counts >= method["groups"]) & (score_unique > 1)]
        grouped = complete.loc[complete.index.get_level_values("datetime").isin(eligible)]
        diagnostics["group_sessions"] = len(eligible)
        diagnostics["group_omitted_sessions"] = diagnostics["sessions"] - len(eligible)
        if grouped.empty:
            notes.append("分组收益：样本不足或截面分数恒定，未定义。")
        else:
            # 与 Qlib 分组函数相同：按分数排序，每组 floor(n/N)，余数不进入组。
            ordered = grouped.sort_values("score", ascending=method["reverse"])
            groups = pd.DataFrame({f"Group{i + 1}": ordered.groupby(level="datetime")["label"].apply(
                lambda values, i=i: values.iloc[len(values) // method["groups"] * i:len(values) // method["groups"] * (i + 1)].mean()
            ) for i in range(method["groups"])})
            groups["long-short"] = groups["Group1"] - groups[f"Group{method['groups']}"]
            groups["long-average"] = groups["Group1"] - grouped.groupby(level="datetime")["label"].mean()
            if len(groups) > 1 and all(groups[name].nunique() > 1 for name in ("long-short", "long-average")):
                produced = model_performance_graph(grouped.copy(), graph_names=["group_return"], **kwargs)
                produced[0].update_layout(title="分组算术累计收益（非资金净值）")
                figures.extend(produced)
            else:
                figures.append(ScatterGraph(groups.cumsum(), layout={"title": "分组算术累计收益（非资金净值）"}).figure)
                notes.append("分组收益分布：样本不足或收益差恒定，省略分布拟合。")
            diagnostics["group_remainder_rows"] = int((counts.loc[eligible] % method["groups"]).sum())
    if "pred_ic" in method["graphs"]:
        eligible = counts.index[(counts >= 2) & (score_unique > 1) & (label_unique > 1)]
        ic_frame = complete.loc[complete.index.get_level_values("datetime").isin(eligible)]
        diagnostics["ic_sessions"] = len(eligible)
        diagnostics["ic_undefined_sessions"] = diagnostics["sessions"] - len(eligible)
        if ic_frame.empty:
            diagnostics["ic_mean"] = diagnostics["rank_ic_mean"] = None
            notes.append("IC/Rank IC：没有有效截面，统计未定义。")
        else:
            series = pd.DataFrame({name: ic_frame.groupby(level="datetime").apply(
                lambda values, corr=corr: values["score"].corr(values["label"], method=corr)
            ) for name, corr in (("IC", "pearson"), ("Rank IC", "spearman"))})
            diagnostics["ic_mean"] = float(series["IC"].mean())
            diagnostics["rank_ic_mean"] = float(series["Rank IC"].mean())
            if len(series) > 2 and series["IC"].nunique() > 1:
                figures.extend(model_performance_graph(ic_frame.copy(), graph_names=["pred_ic"], **kwargs))
            else:
                figures.append(ic_figure(series))
                notes.append("IC 分布：样本不足或 IC 恒定，省略分布拟合；截面 IC 仍展示。")
    if "pred_autocorr" in method["graphs"]:
        shifted = frame.groupby(level="instrument")["score"].shift(method["lag"])
        paired = frame.assign(previous=shifted).dropna(subset=["score", "previous"])
        valid = paired.groupby(level="datetime").filter(lambda values: len(values) >= 2 and values["score"].nunique() > 1 and values["previous"].nunique() > 1)
        diagnostics["autocorr_sessions"] = int(valid.index.get_level_values("datetime").nunique())
        if valid.empty:
            notes.append("预测自相关：样本不足或截面分数恒定，未定义。")
        else:
            figures.extend(model_performance_graph(frame.copy(), graph_names=["pred_autocorr"], **kwargs))
    return figures, diagnostics, notes


def render_qlib_report(context, request: Mapping, *, verification_summary: str) -> str:
    """验证结论与 Qlib 图形并列展示，图形成功不改变验证状态。"""
    request = validate_report_request(request)
    from .portfolio_report import PORTFOLIO_REPORT_VERSION, render_portfolio_report

    if request["contract_version"] == PORTFOLIO_REPORT_VERSION:
        return render_portfolio_report(context, request, verification_summary=verification_summary)
    frame = read_prediction_frame(context, request)
    figures, diagnostics, notes = _model_figures(frame, request["method"])
    from plotly.io import to_html

    sections = []
    for index, figure in enumerate(figures):
        sections.append(to_html(figure, full_html=False, include_plotlyjs=(index == 0)))
    def pre(value):
        return "<pre>" + html.escape(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False)) + "</pre>"
    return (
        '<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><title>Qlib 研究报告</title>'
        '<style>body{font-family:Arial,"Microsoft YaHei",sans-serif;margin:2rem}pre{white-space:pre-wrap;overflow-wrap:anywhere}</style></head><body>'
        '<h1>Qlib 模型研究报告</h1><h2>来源与验证范围</h2>'
        + '<p>图形生成成功不代表独立重训或公式复现验证通过。</p><pre>' + html.escape(verification_summary) + '</pre>'
        + pre({"result_id": context.snapshot.bundle.result_id, "verification_hash": context.verification.verification_hash,
               "verification_status": context.verification.status})
        + '<h2>方法与样本</h2><p>标签采用原始收益；分组按截面分数排序，每组 floor(n/N)，余数不进入组。分组曲线为算术累计收益，非资金净值。</p>'
        + pre(request) + pre(diagnostics) + ''.join('<p>' + html.escape(note) + '</p>' for note in notes)
        + ''.join(sections) + '</body></html>\n'
    )
