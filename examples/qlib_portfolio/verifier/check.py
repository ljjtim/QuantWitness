"""从封存日线独立核对日频模型的时间、样本、选模和预测指标。"""

from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
import json
import math
from pathlib import Path
from statistics import fmean, pstdev

import pyarrow.parquet as pq


LOCAL = timezone(timedelta(hours=8))


def _day(value):
    return date.fromisoformat(str(value)[:10])


def _time(value):
    value = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    if value.tzinfo is None:
        raise ValueError("时间缺少时区")
    return value


def _at(session, hour, minute=0):
    return datetime.combine(session, time(hour, minute), LOCAL)


def _finite(value):
    return value is not None and math.isfinite(float(value))


def _equal(actual, expected):
    if expected is None:
        return actual is None or (isinstance(actual, float) and math.isnan(actual))
    return _finite(actual) and math.isclose(float(actual), expected, rel_tol=1e-10, abs_tol=1e-12)


def _require(condition, message):
    if not condition:
        raise ValueError(message)


def _index(rows, columns):
    result = {}
    for row in rows:
        key = tuple(row[column] for column in columns)
        _require(key not in result, f"重复行键：{columns} {key}")
        result[key] = row
    return result


def _tables(root):
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    result = {}
    for item in manifest["tables"]:
        schema = item["schema_id"]
        table_id = "models" if schema == "research.qlib-model-inventory.v1" else schema.removeprefix("project.qlib_demo.").removesuffix(".v1")
        table_id = table_id.replace("portfolio-targets", "portfolio_targets").replace("decision-benchmarks", "decision_benchmarks").replace("execution-market", "execution_market")
        _require(table_id not in result, "Result 表标识不唯一")
        result[table_id] = pq.read_table([root / path for path in item["files"]]).to_pylist()
    required = {
        "features", "labels", "samples", "split_audit", "fit_audit",
        "validation_predictions", "test_predictions", "selection", "fold_selections",
        "holdout_predictions", "holdout_receipt", "holdout_index", "raw_prices",
        "study_design", "metrics",
    }
    if "holdout_receipt" not in result:
        required -= {"test_predictions", "selection", "fold_selections", "holdout_predictions", "holdout_receipt"}
    _require(required <= result.keys(), f"Result 缺少复核表：{sorted(required - result.keys())}")
    return result


def _inputs(tables, design):
    calendar = [_day(value) for value in design["calendar_sessions"]]
    sessions = [_day(value) for value in design["research_sessions"]]
    entities = list(design["entities"])
    _require(calendar == sorted(set(calendar)), "冻结交易日历不唯一或无序")
    _require(sessions == sorted(set(sessions)) and set(sessions) <= set(calendar), "研究日历不闭合")
    _require(entities and len(set(entities)) == len(entities), "冻结证券集合为空或重复")
    position = {day: index for index, day in enumerate(calendar)}
    raw = {}
    for row in tables["raw_prices"]:
        key = (row["entity_id"], _day(row["session"]))
        _require(key not in raw, f"原始价格重复：{key}")
        _require(key[0] in entities and key[1] in position, "原始价格超出冻结范围")
        raw[key] = row["close"]
    _require(raw, "原始价格为空")
    expected_features, expected_labels = {}, {}
    for entity in entities:
        for session in sessions:
            index = position[session]
            _require(index >= 11 and index + 2 < len(calendar), "研究窗口缺少预热或标签可见日历闭包")
            for window in (5, 10):
                prices = [raw.get((entity, day)) for day in calendar[index - window - 1:index]]
                usable = all(_finite(value) and value > 0 for value in prices)
                returns = [prices[i + 1] / prices[i] - 1 for i in range(window)] if usable else []
                expected_features[(entity, session, "historical_return", window)] = prices[-1] / prices[0] - 1 if usable else None
                expected_features[(entity, session, "volatility", window)] = pstdev(returns) if usable else None
            first = raw.get((entity, session))
            last = raw.get((entity, calendar[index + 1]))
            target = last / first - 1 if _finite(first) and _finite(last) and first > 0 and last > 0 else None
            expected_labels[(entity, session)] = {
                "target": target, "decision_time": _at(session, 9, 30),
                "label_start_time": _at(session, 15),
                "label_end_time": _at(calendar[index + 1], 15),
                "label_available_time": _at(calendar[index + 2], 9, 30),
            }
    features = {}
    for row in tables["features"]:
        key = (row["entity_id"], _day(row["observation_session"]), row["feature_id"], row["window_sessions"])
        _require(key in expected_features and key not in features, f"特征键异常：{key}")
        expected = expected_features[key]
        _require(_equal(row["value"], expected), f"特征数值不符：{key}")
        _require(row["status"] == ("ok" if expected is not None else "missing"), f"特征缺失状态不符：{key}")
        _require(_time(row["observation_time"]) <= _time(row["available_time"]) <= _at(key[1], 9, 30), f"特征可见时间越界：{key}")
        features[key] = row
    _require(features.keys() == expected_features.keys(), "特征未覆盖固定证券、研究日和四项特征全集")
    labels = {}
    for row in tables["labels"]:
        key = (row["entity_id"], _day(row["observation_session"]))
        _require(key in expected_labels and key not in labels, f"标签键异常：{key}")
        expected = expected_labels[key]
        _require(row["horizon_sessions"] == 1 and _equal(row["forward_return"], expected["target"]), f"标签值或期限不符：{key}")
        for field in ("decision_time", "label_start_time", "label_end_time"):
            _require(_time(row[field]) == expected[field], f"标签时间不符：{key} {field}")
        _require(_time(row["available_time"]) == expected["label_available_time"], f"标签成熟时间不符：{key}")
        labels[key] = row
    _require(labels.keys() == expected_labels.keys(), "标签未保留固定输入键全集")
    all_samples = {}
    for (entity, session), label in expected_labels.items():
        available_features = {f"{feature}__w{window}": value for (code, day, feature, window), value in expected_features.items() if code == entity and day == session}
        if label["target"] is not None and any(value is not None for value in available_features.values()):
            sid = f"{entity}:{session.isoformat()}:h1"
            all_samples[sid] = {**label, **available_features, "entity_id": entity, "observation_session": session}
    return all_samples


def _samples(tables, design, all_samples):
    boundary = _time(design["holdout_start"])
    development = {sid: row for sid, row in all_samples.items() if row["label_end_time"] < boundary and row["label_available_time"] <= boundary}
    actual = {key[0]: row for key, row in _index(tables["samples"], ("sample_id",)).items()}
    _require(actual.keys() == development.keys(), "开发样本与可见标签、可用特征交集不一致")
    for sid, row in actual.items():
        expected = development[sid]
        _require(row["entity_id"] == expected["entity_id"] and _day(row["observation_session"]) == expected["observation_session"], f"样本实体或日期不符：{sid}")
        _require(_day(_time(row["observation_time"]).astimezone(LOCAL)) == expected["observation_session"], f"样本观察日错位：{sid}")
        for field in ("decision_time", "label_start_time", "label_end_time", "label_available_time"):
            _require(_time(row[field]) == expected[field], f"样本时间不符：{sid} {field}")
        _require(_time(row["feature_available_time"]) <= expected["decision_time"], f"样本特征存在前视：{sid}")
        for field in ("target", "historical_return__w5", "historical_return__w10", "volatility__w5", "volatility__w10"):
            _require(_equal(row[field], expected[field]), f"样本数值不符：{sid} {field}")
    holdout = {sid: row for sid, row in all_samples.items() if row["decision_time"] >= boundary}
    index = {key[0]: row for key, row in _index(tables["holdout_index"], ("sample_id",)).items()}
    _require(index.keys() == holdout.keys(), "holdout 索引与有效样本全集不一致")
    for sid, row in index.items():
        _require(_time(row["label_available_time"]) == holdout[sid]["label_available_time"], "holdout 索引成熟时间不符")
    return development, holdout


def _splits(tables, design, samples):
    calendar = [_day(value) for value in design["split_calendar_sessions"]]
    calendar = [day for day in calendar if day <= max(row["observation_session"] for row in samples.values())]
    _require(calendar == sorted(set(calendar)), "切分日历不唯一或无序")
    _require((design["train_sessions"], design["validation_sessions"], design["test_sessions"], design["step_sessions"], design["embargo_sessions"], design["expanding"]) == (30, 10, 10, 10, 1, True), "切分设计与批准合同不一致")
    expected, folds = {}, {}
    for offset in range(0, len(calendar) - 50, 10):
        fid = f"walk_forward_{len(folds) + 1:03d}"
        train_days, valid_days = set(calendar[:offset + 30]), set(calendar[offset + 30:offset + 40])
        test_days, embargo_days = set(calendar[offset + 41:offset + 51]), {calendar[offset + 40]}
        grouped = {role: {sid for sid, row in samples.items() if row["observation_session"] in days} for role, days in (("train", train_days), ("validation", valid_days), ("test", test_days), ("embargoed", embargo_days))}
        _require(all(grouped[role] for role in ("train", "validation", "test")), "独立切分出现空窗口")
        cuts = {"train": min(samples[sid]["decision_time"] for sid in grouped["validation"]), "validation": min(samples[sid]["decision_time"] for sid in grouped["test"])}
        grouped["purged"] = set()
        for role, cutoff in cuts.items():
            removed = {sid for sid in grouped[role] if samples[sid]["label_end_time"] >= cutoff or samples[sid]["label_available_time"] > cutoff}
            grouped[role] -= removed
            grouped["purged"] |= removed
        for role, members in grouped.items():
            for sid in members:
                expected[(fid, sid)] = role
        folds[fid] = grouped
    _require(folds, "未构成正式 walk-forward fold")
    audit = _index(tables["split_audit"], ("fold_id", "sample_id"))
    _require(audit.keys() == expected.keys(), "切分审计成员不符")
    for key, row in audit.items():
        _require(row["role"] == expected[key], f"purge/embargo/切分角色不符：{key}")
        _require(row["exclusion_reason"] == (row["role"] if row["role"] in {"purged", "embargoed"} else None), "切分排除原因不符")
        for field, expected_field in (("label_start", "label_start_time"), ("label_end", "label_end_time")):
            _require(_time(row[field]) == samples[key[1]][expected_field], "切分标签区间不符")
    candidates = set(design["candidate_ids"])
    _require(candidates and len(candidates) == len(design["candidate_ids"]), "冻结候选数不一致")
    fits = _index(tables["fit_audit"], ("fold_id", "candidate_id"))
    _require(fits.keys() == {(fid, cid) for fid in folds for cid in candidates}, "三模型拟合证据缺少 fold 或候选")
    for (fid, cid), row in fits.items():
        for role, field in (("train", "train_ids_json"), ("validation", "validation_ids_json")):
            ids = json.loads(row[field])
            _require(len(ids) == len(set(ids)) and set(ids) == folds[fid][role], f"拟合样本范围不符：{fid}/{cid}/{role}")
        _require(row["evidence_scope"] == "processor_train_sample_scope", "处理器拟合证据范围不符")
    return folds, candidates


def _prediction(row, sample, stage):
    _require(row["stage"] == stage and row["horizon_sessions"] == 1, "预测阶段或期限不符")
    _require(row["entity_id"] == sample["entity_id"] and _day(row["observation_session"]) == sample["observation_session"], "预测实体或日期不符")
    for field in ("actual", "raw_label", "evaluation_label"):
        _require(_equal(row[field], sample["target"]), f"预测标签被改变：{field}")
    _require(row["score_semantics"] == "raw_return_prediction" and _finite(row["prediction"]), "预测值非有限或预测语义不符")
    _require(_time(row["feature_available_time"]) <= sample["decision_time"], "预测使用未来特征")
    for field in ("decision_time", "label_start_time", "label_end_time", "label_available_time"):
        _require(_time(row[field]) == sample[field], f"预测时间不符：{field}")
    return (float(row["prediction"]) - sample["target"]) ** 2


def _predictions(tables, design, samples, holdout, folds, candidates):
    valid = _index(tables["validation_predictions"], ("fold_id", "candidate_id", "sample_id"))
    _require(valid.keys() == {(fid, cid, sid) for fid in folds for cid in candidates for sid in folds[fid]["validation"]}, "validation 预测成员不完整或越界")
    scores, visible, ended = {}, {}, {}
    for fid, groups in folds.items():
        visible[fid] = max(samples[sid]["label_available_time"] for sid in groups["validation"])
        ended[fid] = max(samples[sid]["label_end_time"] for sid in groups["validation"])
        for cid in candidates:
            scores[(fid, cid)] = -fmean(_prediction(valid[(fid, cid, sid)], samples[sid], "validation") for sid in groups["validation"])
    aggregate = {cid: fmean(scores[(fid, cid)] for fid in folds) for cid in candidates}
    winner = min(candidates, key=lambda cid: (-aggregate[cid], cid))
    _require(len(tables["selection"]) == 1, "最终选择不唯一")
    selection = tables["selection"][0]
    _require(selection["selected_candidate_id"] == winner and _equal(selection["validation_metric"], aggregate[winner]), "最终赢家不是 validation fold 均值最优候选")
    _require(selection["objective"] == "neg_mean_squared_error" and selection["direction"] == "maximize", "选模目标不符")
    _require(_time(selection["selection_time"]) == max(visible.values()) <= _time(design["holdout_start"]), "最终选择时点不符或进入 holdout")
    _require(selection["selection_scope"] == "subsequent_locked_holdout", "最终选择用途不符")
    test = _index(tables["test_predictions"], ("fold_id", "sample_id"))
    _require(test.keys() == {(fid, sid) for fid in folds for sid in folds[fid]["test"]}, "test 预测成员不完整或重复候选")
    selections = _index(tables["fold_selections"], ("fold_id",))
    _require(selections.keys() == {(fid,) for fid in folds}, "逐 fold 选择证据不完整")
    test_scores = []
    for fid, groups in folds.items():
        cutoff = min(samples[sid]["decision_time"] for sid in groups["test"])
        known = [other for other in folds if visible[other] <= cutoff and ended[other] < cutoff]
        _require(known and fid in known, "test 开始前当前 validation 尚未成熟")
        local = {cid: fmean(scores[(other, cid)] for other in known) for cid in candidates}
        selected = min(candidates, key=lambda cid: (-local[cid], cid))
        record = selections[(fid,)]
        _require(record["selected_candidate_id"] == selected and _equal(record["validation_metric"], local[selected]), "逐时点赢家未由当时可见 validation 选出")
        _require(_time(record["selection_time"]) == cutoff and record["validation_fold_count"] == len(known), "逐 fold 选择时间或指标范围不符")
        losses = []
        for sid in groups["test"]:
            row = test[(fid, sid)]
            _require(row["candidate_id"] == selected, "test 预测使用未选中候选")
            losses.append(_prediction(row, samples[sid], "test"))
        test_scores.append(-fmean(losses))
    _require(_equal(selection["test_metric"], fmean(test_scores)), "test 汇总指标不符")
    predictions = {key[0]: row for key, row in _index(tables["holdout_predictions"], ("sample_id",)).items()}
    _require(predictions.keys() == holdout.keys() and predictions, "holdout 预测成员不完整")
    losses = []
    for sid, row in predictions.items():
        _require(row["candidate_id"] == winner and row["fold_id"] == "locked_holdout", "holdout 暴露给未选中候选")
        losses.append(_prediction(row, holdout[sid], "holdout"))
    mse = fmean(losses)
    _require(len(tables["holdout_receipt"]) == 1, "holdout 收据不唯一")
    receipt = tables["holdout_receipt"][0]
    _require(receipt["status"] == "committed" and receipt["objective"] == "neg_mean_squared_error" and _equal(receipt["metric"], -mse), "holdout 收据状态或指标不符")
    _require(all(receipt.get(field) for field in ("holdout_identity_hash", "plan_hash", "prepared_hash", "opened_hash", "terminal_hash", "result_hash")), "holdout 收据缺少持久访问阶段证据")
    _require(len(tables["metrics"]) == 1, "正式指标不唯一")
    metric = tables["metrics"][0]
    _require(metric["metric_ref"] == "project.qlib_demo.prediction_mse@1.0.0" and _equal(metric["value"], mse), "正式 MSE 无法从 holdout 预测独立复算")


def verify(context, input_root):
    findings = []
    try:
        tables = _tables(Path(input_root))
        _require(len(tables["study_design"]) == 1, "研究设计不唯一")
        design = json.loads(tables["study_design"][0]["design_json"])
        all_samples = _inputs(tables, design)
        samples, holdout = _samples(tables, design, all_samples)
        folds, candidates = _splits(tables, design, samples)
        if design.get("mode") == "development":
            _require(not {"test_predictions", "selection", "holdout_predictions", "holdout_receipt"} & tables.keys(), "开发结果不得封存test或holdout评估")
            valid = _index(tables["validation_predictions"], ("fold_id", "candidate_id", "sample_id"))
            _require(valid.keys() == {(fid,cid,sid) for fid in folds for cid in candidates for sid in folds[fid]["validation"]}, "开发validation成员不完整")
            metric = tables["metrics"][0]
            _require(metric["stage"] == "validation", "开发指标阶段不符")
            _require(metric["session"] == max(str(r["observation_session"]) for r in valid.values()), "开发指标日期不符")
            _require(metric["available_at"] == max(r["label_available_time"] for r in valid.values()), "开发指标可见时点不符")
            losses = [_prediction(row, samples[sid], "validation") for (fid,cid,sid),row in valid.items()]
            _require(_equal(tables["metrics"][0]["value"], fmean(losses)), "开发validation MSE不一致")
        else:
            _predictions(tables, design, samples, holdout, folds, candidates)
            if design.get("mode") == "portfolio":
                from portfolio import verify_portfolio
                verify_portfolio(tables, design)
    except (ValueError, KeyError, TypeError, IndexError, ZeroDivisionError) as exc:
        findings.append(f"日频模型独立复核失败：{exc}")
    return {
        "contract_version": "project-verifier-output-v1",
        "status": "fail" if findings else "pass",
        "result_id": context["result_id"],
        "findings": findings,
        "evidence_hashes": {},
    }
