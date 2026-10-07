"""从 Result 绑定事实独立复核日频模型的时间切分、选模和 holdout。

model_diagnostics 字段：mode、design、tables、table_bindings、model_configs、
holdout_ledger。model_configs 按 models 表 config_path 索引；holdout_ledger
包含 plan、prepared、opened、terminal 原始记录。所有时间均使用带时区 ISO 文本。
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from datetime import date, datetime
from statistics import fmean

from research_pipeline.platform import typed_canonical_hash

from .errors import EvidenceContractError
from .model_sequence_validity import verify_sequence_research_facts, sequence_expected_parameters

MODEL_DIAGNOSTICS_MODE = "walk_forward_prediction_v1"
MODEL_DEVELOPMENT_MODE = "walk_forward_development_v1"


def _require(condition, message):
    if not condition:
        raise EvidenceContractError(message)


def _time(value):
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    result = value if isinstance(value, datetime) else datetime.fromisoformat(text)
    _require(result.tzinfo is not None, "模型事实时间缺少时区")
    return result


def _day(value):
    return date.fromisoformat(str(value)[:10])


def _equal(actual, expected):
    return actual is not None and math.isfinite(float(actual)) and math.isclose(float(actual), float(expected), rel_tol=1e-10, abs_tol=1e-12)


def _index(rows, columns):
    result = {}
    for row in rows:
        key = tuple(row[column] for column in columns)
        _require(key not in result, "模型事实存在重复行键")
        result[key] = row
    return result


def _json_value(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def verify_model_result_binding(snapshot, facts):
    """事实必须逐行绑定已验证 Result；不得用项目声明替换正式表和封存配置。"""
    model = facts.get("model_diagnostics")
    if model is None:
        return
    _require(isinstance(model, Mapping) and model.get("mode") in {MODEL_DIAGNOSTICS_MODE, MODEL_DEVELOPMENT_MODE}, "模型有效性模式无效")
    tables, bindings = model["tables"], model["table_bindings"]
    _require(set(tables) == set(bindings), "模型事实表未完整绑定 Result")
    for name, schema in bindings.items():
        actual = _json_value(snapshot.read_table(schema).to_pylist())
        _require(actual == tables[name], f"模型事实与 Result 表不一致：{name}")
    model_table = snapshot.table_manifest(bindings["models"])
    holdout_table = snapshot.table_manifest(bindings["holdout_receipt"]) if "holdout_receipt" in bindings else None
    support = {(item.artifact_key, item.source_path): item for item in snapshot.bundle.support_files}

    def read_support(artifact, source):
        item = support.get((artifact, source))
        _require(item is not None, f"模型 Result 缺少封存文件：{source}")
        raw = snapshot.support_bytes.get(item.relative_path)
        _require(raw is not None, f"模型 Result 文件未经本次快照验证：{source}")
        return json.loads(raw)

    configs = model["model_configs"]
    _require(set(configs) == {row["config_path"] for row in tables["models"]}, "模型配置事实不完整")
    for path, config in configs.items():
        _require(read_support(model_table.artifact_key, path) == config, "模型配置事实与封存文件不一致")
        if config["candidate"]["model"]["class"] == "GeneratedModel":
            from .generated_model_validity import verify_generated_support
            verify_generated_support(snapshot, model_table.artifact_key, config)
        if config["candidate"]["model"]["class"] in {"GRU", "LSTM", "TransformerModel"}:
            import pyarrow as pa
            import pyarrow.parquet as pq
            weights = support.get((model_table.artifact_key, config["weights_path"]))
            _require(weights is not None, "序列模型 Result缺少显式权重文件")
            windows = model.get("model_window_facts", {}).get(path)
            _require(isinstance(windows, Mapping), "序列模型缺少窗口文件事实")
            for name, source in config["sequence"]["files"].items():
                item = support.get((model_table.artifact_key, source))
                _require(item is not None, "序列模型 Result缺少窗口文件")
                raw = snapshot.support_bytes.get(item.relative_path)
                _require(raw is not None, "序列模型窗口文件未经本次快照验证")
                actual = _json_value(pq.read_table(pa.BufferReader(raw)).to_pylist())
                _require(actual == windows.get(name), "序列模型窗口事实与封存文件不一致")
    for name in (("plan", "prepared", "opened", "terminal") if holdout_table is not None else ()):
        actual = read_support(holdout_table.artifact_key, f"holdout-ledger/{name}.json")
        _require(actual == model["holdout_ledger"][name], "holdout 事实与封存账本不一致")
    if "study_design" in tables:
        _require(len(tables["study_design"]) == 1 and json.loads(tables["study_design"][0]["design_json"]) == model["design"], "模型设计与 Result 不一致")


def _split(model):
    tables, design = model["tables"], model["design"]
    samples = {key[0]: row for key, row in _index(tables["samples"], ("sample_id",)).items()}
    _require(samples, "模型开发样本为空")
    horizon = design.get("horizon_sessions", 1)
    _require(type(horizon) is int and horizon > 0, "冻结模型期限无效")
    _require(all(row.get("horizon_sessions", 1) == horizon for row in samples.values()), "模型开发样本期限与冻结设计不一致")
    boundary = _time(design["holdout_start"])
    for row in samples.values():
        _require(_time(row["feature_available_time"]) <= _time(row["decision_time"]) <= _time(row["label_start_time"]) <= _time(row["label_end_time"]) < boundary, "模型特征前视或标签区间无效")
        _require(_time(row["label_end_time"]) <= _time(row["label_available_time"]) <= boundary, "模型开发标签尚未成熟")
    calendar = [_day(day) for day in design["split_calendar_sessions"]]
    _require(calendar == sorted(set(calendar)), "模型切分日历无序或重复")
    calendar = [day for day in calendar if day <= max(_day(_time(row["observation_time"])) for row in samples.values())]
    train, valid, test, step = (design[name] for name in ("train_sessions", "validation_sessions", "test_sessions", "step_sessions"))
    embargo = design["embargo_sessions"]
    _require(all(type(v) is int and v > 0 for v in (train, valid, test, step)) and type(embargo) is int and embargo >= 0, "模型切分窗口无效")
    expected, folds = {}, {}
    for offset in range(0, len(calendar) - train - valid - test - embargo + 1, step):
        fid = f"walk_forward_{len(folds) + 1:03d}"
        train_start = 0 if design["expanding"] else offset
        boundary_valid = offset + train
        boundary_test = boundary_valid + valid + embargo
        windows = {"train": calendar[train_start:boundary_valid], "validation": calendar[boundary_valid:boundary_valid + valid], "embargoed": calendar[boundary_valid + valid:boundary_test], "test": calendar[boundary_test:boundary_test + test]}
        groups = {role: {sid for sid, row in samples.items() if _day(_time(row["observation_time"])) in days} for role, days in windows.items()}
        _require(all(groups[role] for role in ("train", "validation", "test")), "模型切分窗口为空")
        cutoffs = {"train": min(_time(samples[sid]["decision_time"]) for sid in groups["validation"]), "validation": min(_time(samples[sid]["decision_time"]) for sid in groups["test"])}
        groups["purged"] = set()
        for role, cutoff in cutoffs.items():
            removed = {sid for sid in groups[role] if _time(samples[sid]["label_end_time"]) >= cutoff or _time(samples[sid]["label_available_time"]) > cutoff}
            groups[role] -= removed
            groups["purged"] |= removed
        _require(groups["train"] and groups["validation"], "模型 purge 后样本为空")
        for role, ids in groups.items():
            expected.update({(fid, sid): role for sid in ids})
        folds[fid] = groups
    _require(folds, "模型没有完整切分窗口")
    audit = _index(tables["split_audit"], ("fold_id", "sample_id"))
    _require(audit.keys() == expected.keys(), "模型切分成员不一致")
    for key, row in audit.items():
        _require(row["role"] == expected[key], "模型 purge 或 embargo 角色不一致")
        _require(row["exclusion_reason"] == (row["role"] if row["role"] in {"purged", "embargoed"} else None), "模型排除原因不一致")
        for field, source in (("label_start", "label_start_time"), ("label_end", "label_end_time")):
            _require(_time(row[field]) == _time(samples[key[1]][source]), "模型切分标签时间不一致")
    candidates = set(design["candidate_ids"])
    _require(candidates and len(candidates) == len(design["candidate_ids"]), "模型候选重复或为空")
    fits = _index(tables["fit_audit"], ("fold_id", "candidate_id"))
    _require(fits.keys() == {(fid, cid) for fid in folds for cid in candidates}, "模型拟合证据未覆盖候选和窗口")
    for (fid, cid), row in fits.items():
        for role, field in (("train", "train_ids_json"), ("validation", "validation_ids_json")):
            ids = json.loads(row[field])
            _require(len(ids) == len(set(ids)) and set(ids) == folds[fid][role], "模型实际拟合范围不一致")
        _require(row["evidence_scope"] == "processor_train_sample_scope", "模型处理器拟合范围未封存")
    return samples, folds, candidates


def _prediction(row, sample, stage):
    _require(row["stage"] == stage and row["entity_id"] == sample["entity_id"] and _day(row["observation_session"]) == _day(sample["observation_session"]), "模型预测身份或阶段不一致")
    _require(row["horizon_sessions"] == sample.get("horizon_sessions", 1), "模型预测期限不一致")
    for field in ("actual", "raw_label", "evaluation_label"):
        _require(_equal(row[field], sample["target"]), "模型预测标签被改变")
    _require(row["score_semantics"] == "raw_return_prediction" and math.isfinite(float(row["prediction"])), "模型预测值或语义无效")
    _require(_time(row["feature_available_time"]) <= _time(sample["decision_time"]), "模型预测包含未来特征")
    for field in ("decision_time", "label_start_time", "label_end_time", "label_available_time"):
        _require(_time(row[field]) == _time(sample[field]), "模型预测时间与样本不一致")
    return (float(row["prediction"]) - float(sample["target"])) ** 2


def _selection(model, samples, folds, candidates):
    tables, design = model["tables"], model["design"]
    valid = _index(tables["validation_predictions"], ("fold_id", "candidate_id", "sample_id"))
    _require(valid.keys() == {(fid, cid, sid) for fid in folds for cid in candidates for sid in folds[fid]["validation"]}, "模型 validation 成员不完整")
    scores, visible, ended = {}, {}, {}
    for fid, groups in folds.items():
        visible[fid] = max(_time(samples[sid]["label_available_time"]) for sid in groups["validation"])
        ended[fid] = max(_time(samples[sid]["label_end_time"]) for sid in groups["validation"])
        for cid in candidates:
            scores[fid, cid] = -fmean(_prediction(valid[fid, cid, sid], samples[sid], "validation") for sid in groups["validation"])
    aggregate = {cid: fmean(scores[fid, cid] for fid in folds) for cid in candidates}
    winner = min(candidates, key=lambda cid: (-aggregate[cid], cid))
    _require(len(tables["selection"]) == 1, "模型最终选择不唯一")
    selection = tables["selection"][0]
    _require(selection["selected_candidate_id"] == winner and _equal(selection["validation_metric"], aggregate[winner]), "模型赢家不符合 validation 排序")
    _require(selection["objective"] == "neg_mean_squared_error" and selection["direction"] == "maximize" and selection["selection_scope"] == "subsequent_locked_holdout", "模型选模目标或用途不符")
    _require(_time(selection["selection_time"]) == max(visible.values()) <= _time(design["holdout_start"]), "模型最终选模时点不一致")
    test = _index(tables["test_predictions"], ("fold_id", "sample_id"))
    selections = _index(tables["fold_selections"], ("fold_id",))
    _require(test.keys() == {(fid, sid) for fid in folds for sid in folds[fid]["test"]} and selections.keys() == {(fid,) for fid in folds}, "模型 test 预测或逐窗口选择不完整")
    test_scores = []
    for fid, groups in folds.items():
        cutoff = min(_time(samples[sid]["decision_time"]) for sid in groups["test"])
        known = [other for other in folds if visible[other] <= cutoff and ended[other] < cutoff]
        _require(fid in known, "模型当前 validation 在 test 开始前未成熟")
        local = {cid: fmean(scores[other, cid] for other in known) for cid in candidates}
        selected = min(candidates, key=lambda cid: (-local[cid], cid))
        record = selections[fid,]
        _require(record["selected_candidate_id"] == selected and _equal(record["validation_metric"], local[selected]), "模型逐时点赢家不一致")
        _require(_time(record["selection_time"]) == cutoff and record["validation_fold_count"] == len(known), "模型逐时点选择使用未来窗口")
        losses = []
        for sid in groups["test"]:
            row = test[fid, sid]
            _require(row["candidate_id"] == selected, "模型 test 使用未选中候选")
            losses.append(_prediction(row, samples[sid], "test"))
        test_scores.append(-fmean(losses))
    _require(_equal(selection["test_metric"], fmean(test_scores)), "模型 test 指标不一致")
    return selection

MODEL_VERIFIER_ALGORITHM_VERSIONS = {
    "data.pit": "verifier.data-pit.v1",
    "label.split": "verifier.model-label-split.v2",
    "search.holdout": "verifier.model-search-holdout.v2",
    "statistics": "verifier.model-statistics.v1",
    "financial.tradability": "verifier.model-financial-scope.v2",
}


def _fit_configs(model, samples, folds, selection=None):
    tables, design = model["tables"], model["design"]
    candidates = json.loads(design["candidate_parameters_json"])
    _require(set(candidates) == set(design["candidate_ids"]), "模型配置候选清单不一致")
    winner = None if selection is None else selection["selected_candidate_id"]
    if selection is not None:
        _require(json.loads(selection["selected_parameters_json"]) == candidates[winner], "模型选中参数不一致")
    models = _index(tables["models"], ("fold_id", "candidate_id"))
    expected = {(fid, cid) for fid in folds for cid in candidates}
    if winner is not None:
        expected.add(("locked_holdout", winner))
    _require(models.keys() == expected, "模型封存集合未覆盖开发模型与唯一赢家")
    _require(set(model["model_configs"]) == {row["config_path"] for row in models.values()}, "模型配置未完整封存")
    sessions = sorted({_day(row["observation_session"]) for row in samples.values()})
    width = design["validation_sessions"]
    _require(len(sessions) > width, "最终模型没有完整训练区间")
    final_valid = {sid for sid, row in samples.items() if _day(row["observation_session"]) in sessions[-width:]}
    cutoff = min(_time(samples[sid]["decision_time"]) for sid in final_valid)
    final_train = {sid for sid, row in samples.items() if _day(row["observation_session"]) < sessions[-width] and _time(row["label_end_time"]) < cutoff and _time(row["label_available_time"]) <= cutoff}
    for (fid, cid), row in models.items():
        config = model["model_configs"][row["config_path"]]
        candidate = candidates[cid]
        _require(config["candidate"] == candidate, "实际模型配置不符合冻结候选")
        _require(row["model_class"] == candidate["model"]["module_path"] + "." + candidate["model"]["class"] and row["status"] == "fitted", "模型类型或状态不一致")
        _require(config["root_seed"] == design["root_seed"] and config["training_label"] == "raw", "模型种子或标签口径不符")
        _require(config["thread_count"] == 1 and row["target_kind"] == "regression", "模型资源或任务类型不符")
        _require(config["model_path"] == row["model_path"] and config["feature_columns"] == json.loads(row["feature_columns_json"]), "模型文件或特征列索引不符")
        kwargs, fit = dict(candidate["model"]["kwargs"]), dict(candidate["fit"])
        if candidate["model"]["class"] in {"GRU", "LSTM", "TransformerModel"}:
            kwargs, fit = sequence_expected_parameters(config, candidate, design)
        elif candidate["model"]["class"] == "GeneratedModel":
            kwargs.update(d_feat=len(config["feature_columns"]), seed=design["root_seed"])
            _require(config.get("generated", {}).get("definition") == kwargs["definition"], "生成模型结构与冻结声明不符")
            _require(config.get("versions", {}).get("torch", "").split("+")[0] == "2.5.1", "生成模型Torch版本不符")
            _require(config.get("training_curve") and all(row["metric"] == "mse" and math.isfinite(row["value"]) and row["value"] >= 0 for row in config["training_curve"]), "生成模型训练曲线无效")
        elif candidate["model"]["class"] == "LGBModel":
            kwargs.update(num_threads=1, seed=design["root_seed"], device_type="cpu")
            fit.setdefault("verbose_eval", 0)
        elif candidate["model"]["class"] == "DEnsembleModel":
            kwargs.update(num_threads=1, seed=design["root_seed"], device_type="cpu")
            count = kwargs.get("num_models", 6)
            state = config.get("ensemble_state", {})
            _require(kwargs.get("enable_sr", True) is True and kwargs.get("enable_fs") is False,
                     "Double Ensemble 只准入样本重加权版本")
            _require(state.get("num_models") == count
                     and state.get("sub_features") == [config["feature_columns"]] * count
                     and state.get("sub_weights") == (kwargs.get("sub_weights") or [1] * count),
                     "Double Ensemble 子模型状态与声明不一致")
            iterations = state.get("iterations", [])
            _require(len(iterations) == count
                     and all(type(n) is int and 1 <= n <= kwargs.get("epochs", 100) for n in iterations),
                     "Double Ensemble 训练轮数不符")
            _require(config.get("training_curve") == [] and not fit,
                     "Double Ensemble 不能声明未导出的训练曲线或 fit 参数")
        elif candidate["model"]["class"] == "XGBModel":
            kwargs.update(nthread=1, seed=design["root_seed"], device="cpu", objective="reg:squarederror")
            fit.update(early_stopping_rounds=None)
            fit.setdefault("verbose_eval", False)
        _require(config["effective_model_kwargs"] == kwargs and config["effective_fit_kwargs"] == fit, "模型实际训练参数不符合冻结声明")
        for phase in ("infer", "learn"):
            _require(len(config["processor_files"][phase]) == len(candidate["processors"][phase]), "模型处理器配置数量不符")
        train, valid = (final_train, final_valid) if fid == "locked_holdout" else (folds[fid]["train"], folds[fid]["validation"])
        for field, ids in (("train_ids", train), ("valid_ids", valid)):
            actual = config[field]
            _require(actual and len(actual) == len(set(actual)) and set(actual) == ids, "实际模型或处理器拟合范围不一致")
        _require(not train & valid, "实际模型训练与 validation 重叠")
        fit_time = max(min(_time(samples[sid]["decision_time"]) for sid in valid), max(_time(samples[sid]["label_end_time"]) for sid in valid), max(_time(samples[sid]["label_available_time"]) for sid in valid))
        _require(_time(config["fit_time"]) == _time(row["fit_time"]) == fit_time <= _time(design["holdout_start"]), "实际模型拟合时点不符")
        expected_scope = "holdout-final-development" if fid == "locked_holdout" else next(item["fit_scope_ref"] for item in tables["fit_audit"] if item["fold_id"] == fid and item["candidate_id"] == cid)
        _require(config["fit_scope_ref"] == row["fit_scope_ref"] == expected_scope, "模型拟合范围身份不符")


def _holdout(model, selection):
    tables, design = model["tables"], model["design"]
    predictions = tables["holdout_predictions"]
    horizon = design.get("horizon_sessions", 1)
    _require(type(horizon) is int and horizon > 0, "冻结模型期限无效")
    _require(all(row.get("horizon_sessions") == horizon for row in predictions), "holdout预测期限与冻结设计不一致")
    index = _index(tables["holdout_index"], ("sample_id",))
    rows = _index(predictions, ("sample_id",))
    _require(rows and rows.keys() == index.keys(), "holdout 预测与冻结索引不一致")
    boundary = _time(design["holdout_start"])
    losses = []
    for key, row in rows.items():
        _require(row["candidate_id"] == selection["selected_candidate_id"] and row["fold_id"] == "locked_holdout" and row["stage"] == "holdout", "holdout 使用未选中候选")
        _require(_time(row["decision_time"]) >= boundary and _time(row["feature_available_time"]) <= _time(row["decision_time"]), "holdout 范围或特征时间无效")
        _require(_time(row["label_end_time"]) <= _time(row["label_available_time"]) == _time(index[key]["label_available_time"]), "holdout 标签成熟时间不一致")
        _require(_equal(row["actual"], row["raw_label"]) and _equal(row["actual"], row["evaluation_label"]), "holdout 标签口径不一致")
        _require(math.isfinite(float(row["prediction"])) and math.isfinite(float(row["actual"])), "holdout 数值无效")
        losses.append((float(row["prediction"]) - float(row["actual"])) ** 2)
    _require(len(tables["holdout_receipt"]) == 1, "holdout 收据不唯一")
    receipt = tables["holdout_receipt"][0]
    _require(receipt["status"] == "committed" and receipt["objective"] == "neg_mean_squared_error" and _equal(receipt["metric"], -fmean(losses)), "holdout 收据指标不一致")
    ledger = model["holdout_ledger"]
    _require(set(ledger) == {"plan", "prepared", "opened", "terminal"}, "holdout 账本阶段不完整")
    plan, prepared, opened, terminal = (ledger[name] for name in ("plan", "prepared", "opened", "terminal"))
    freeze = plan["freeze_payload"]
    research_id = typed_canonical_hash(design)
    candidate_hash = typed_canonical_hash(json.loads(selection["selected_parameters_json"]))
    _require(freeze["parent_research_purpose"] == research_id and freeze["mode"] == "single_candidate_confirmation" and freeze["candidates"] == [candidate_hash], "holdout 未冻结当前研究唯一候选")
    _require(selection["selected_candidate_id"] == "candidate_" + candidate_hash[:16], "holdout 候选参数身份不符")
    _require(freeze["failure_policy"] == "opened_then_failure_is_consumed" and freeze["random_protocol"]["seed"] == design["root_seed"], "holdout 消费策略或种子不符")
    _require(freeze["validation"]["rule"] == {"selection_hash": selection["selection_hash"], "objective": "neg_mean_squared_error"}, "holdout 未绑定正式选模")
    ids = sorted(key[0] for key in rows)
    split = freeze["holdout_split"]
    _require(split["sample_ids"] == ids and split["start"] == boundary.date().isoformat() and split["end"] == max(str(row["observation_session"])[:10] for row in predictions), "holdout 账本样本范围不一致")
    identity = typed_canonical_hash({"parent_research_purpose": research_id, "data_snapshot": freeze["data_snapshot"], "holdout_start": split["start"], "holdout_end": split["end"], "sample_ids": ids})
    _require(plan["holdout_identity_hash"] == receipt["holdout_identity_hash"] == identity, "holdout 内容身份不符")
    _require(plan["contract_version"] == "research-persistent-holdout-plan-v2" and plan["state"] == "frozen", "holdout 计划合同不符")
    payload = {key: plan[key] for key in ("freeze_payload", "actor", "reason", "unlock_at", "holdout_identity_hash")}
    _require(typed_canonical_hash(payload) == plan["plan_hash"] == receipt["plan_hash"], "holdout 计划内容不符")
    token = typed_canonical_hash({"domain": "locked-holdout-token-v2", "plan_hash": plan["plan_hash"]})
    _require(token == plan["token_hash"] == opened["token_hash"], "holdout 令牌不符")
    for name in ("prepared", "opened", "terminal"):
        value = ledger[name]
        field = name + "_hash"
        _require(value["contract_version"] == f"research-persistent-holdout-{name}-v2" and typed_canonical_hash({key: item for key, item in value.items() if key != field}) == value[field] == receipt[field], "holdout 阶段内容身份不符")
    _require(prepared["state"] == "prepared" and prepared["plan_hash"] == plan["plan_hash"] and prepared["preflight"], "holdout 预检未闭合")
    _require(opened["state"] == "opened" and opened["plan_hash"] == plan["plan_hash"] and opened["prepared_hash"] == prepared["prepared_hash"], "holdout 打开前序记录不符")
    _require(opened["actor"] == plan["actor"] and opened["reason"] == plan["reason"] and opened["sample_ids_hash"] == typed_canonical_hash(ids), "holdout 打开授权或样本不符")
    _require(_time(opened["opened_at"]) >= _time(plan["unlock_at"]) and _time(opened["opened_at"]) >= _time(prepared["prepared_at"]), "holdout 提前打开")
    _require(opened["opening_id"] == "holdout:" + identity[:16], "holdout 打开身份不符")
    _require(terminal["opened_hash"] == opened["opened_hash"] and terminal["status"] == "committed" and terminal["reason"] is None and terminal["result_hash"] == receipt["result_hash"], "holdout 终态结果不符")
    payload = _json_value(predictions)
    _require(typed_canonical_hash({"predictions": payload, "objective": receipt["objective"], "metric": receipt["metric"]}) == receipt["result_hash"], "holdout 预测未绑定持久收据")
    _require(all(_time(row["label_available_time"]) <= _time(opened["opened_at"]) for row in predictions), "holdout 读取尚未成熟标签")
    return fmean(losses), len(losses)


def _statistics(model, statistics, mse, count):
    _require(statistics["method"] == "prediction_mse" and type(statistics["sample_count"]) is int and statistics["sample_count"] == count and _equal(statistics["mse"], mse), "模型诊断统计摘要不一致")
    metrics = model["tables"]["metrics"]
    _require(len(metrics) == 1, "模型正式指标不唯一")
    metric = metrics[0]
    _require(metric["metric_ref"] == statistics["metric_ref"] and _equal(metric["value"], mse), "模型正式 MSE 不一致")
    sessions = [_day(row["observation_session"]) for row in model["tables"]["holdout_predictions"]]
    _require(metric["unit"] == "squared_decimal_price_change" and metric["status"] == "computed", "模型指标单位或计算状态不符")
    _require(type(metric["sample_size"]) is int and metric["sample_size"] == count, "模型指标样本数不符")
    _require(_day(metric["sample_start"]) == min(sessions) and _day(metric["sample_end"]) == max(sessions), "模型指标样本日期范围不符")


def _financial_tradability(financial, bar_tca_expectations):
    """组合声明必须与正式 Result 金融 oracle 的独立重算一致。"""
    prediction_only = {
        "applicability": "not_applicable",
        "reason": "prediction_diagnostics_has_no_trading_simulation",
    }
    if financial == prediction_only:
        _require(bar_tca_expectations is None, "模型组合结果不得声明无交易仿真")
        return
    _require(isinstance(financial, Mapping), "模型金融事实缺失")
    _require(set(financial) == {
        "applicability", "mode", "simulation_result_hash", "source_ledger_hash", "bar_tca",
    }, "模型组合金融事实字段不完整")
    _require(financial["applicability"] == "applicable" and financial["mode"] == "daily_cash_simulation_v1", "模型组合仿真模式无效")
    _require(isinstance(bar_tca_expectations, Mapping), "模型组合缺少正式 Result 金融 oracle")
    _require(set(bar_tca_expectations) == {
        "tca_result_hash", "tca_artifact_manifest_hash", "tca_policy_hash",
        "tca_input_hash", "tca_implementation_digest", "tca_rule_snapshot_hash",
        "tca_source_simulation_hash", "tca_source_ledger_hash", "tca_source_fill_manifest_hash",
        "claim_ceiling", "reconciliation_delta_units", "liquidity_attribution_status",
    }, "模型组合金融 oracle 事实不完整")
    _require(financial["bar_tca"] == bar_tca_expectations, "模型组合 TCA 与正式 Result 独立重算不一致")
    _require(financial["simulation_result_hash"] == bar_tca_expectations["tca_source_simulation_hash"], "模型组合仿真引用与正式 Result 不一致")
    _require(financial["source_ledger_hash"] == bar_tca_expectations["tca_source_ledger_hash"], "模型组合账本引用与正式 Result 不一致")


def recompute_model_validity_issues(facts, *, bar_tca_expectations=None):
    """预测误差与组合金融事实分别复核，组合事实绑定正式 Result oracle。"""
    issues = {gate: set() for gate in ("data.pit", "label.split", "search.holdout", "statistics", "financial.tradability")}
    model = facts["model_diagnostics"]
    errors = (EvidenceContractError, KeyError, TypeError, ValueError, OverflowError, ZeroDivisionError)
    try:
        _require(model["mode"] in {MODEL_DIAGNOSTICS_MODE, MODEL_DEVELOPMENT_MODE}, "模型诊断模式无效")
        _require(facts.get("label_split") == {"mode": model["mode"]} and facts.get("search_holdout") == {"mode": model["mode"]}, "模型门禁模式不一致")
        verify_sequence_research_facts(model)
        samples, folds, candidates = _split(model)
    except errors:
        issues["label.split"].add("label.leakage")
        issues["search.holdout"].add("search.ledger_incomplete")
        issues["statistics"].add("statistics.method_not_applicable")
        return issues
    try:
        if model["mode"] == MODEL_DEVELOPMENT_MODE:
            mse, count = _development(model, samples, folds, candidates)
        else:
            selection = _selection(model, samples, folds, candidates)
            _fit_configs(model, samples, folds, selection)
            mse, count = _holdout(model, selection)
    except errors:
        issues["search.holdout"].add("holdout.access_invalid")
        issues["statistics"].add("statistics.method_not_applicable")
        return issues
    try:
        if model["mode"] == MODEL_DEVELOPMENT_MODE:
            _development_statistics(model, facts["statistics"], mse, count)
        else:
            _statistics(model, facts["statistics"], mse, count)
    except errors:
        issues["statistics"].add("statistics.method_not_applicable")
    try:
        _financial_tradability(facts.get("financial_tradability"), bar_tca_expectations)
    except errors:
        issues["financial.tradability"].add("financial.bar_tca_invalid")
    return issues


def _development(model, samples, folds, candidates):
    """开发区仅验证validation，不选择test赢家或读取holdout结果。"""
    tables = model["tables"]
    _require(not {"test_predictions", "selection", "fold_selections", "holdout_predictions", "holdout_receipt"} & tables.keys(), "开发结果不得包含test或holdout评估")
    _require(model.get("holdout_ledger") == {}, "开发研究不得打开holdout账本")
    _require(not tables.get("holdout_index"), "开发研究不得读取holdout索引")
    _fit_configs(model, samples, folds)
    rows = _index(tables["validation_predictions"], ("fold_id", "candidate_id", "sample_id"))
    expected = {(fid, cid, sid) for fid in folds for cid in candidates for sid in folds[fid]["validation"]}
    _require(rows.keys() == expected, "开发validation成员不完整")
    losses = [_prediction(row, samples[sid], "validation") for (fid, cid, sid), row in rows.items()]
    return fmean(losses), len(losses)


def _development_statistics(model, statistics, mse, count):
    _require(statistics["method"] == "validation_mse" and statistics["sample_count"] == count and _equal(statistics["mse"], mse), "开发validation统计不一致")
    metrics = model["tables"]["metrics"]
    _require(len(metrics) == 1, "开发指标必须唯一")
    metric = metrics[0]
    _require(metric["metric_ref"] == statistics["metric_ref"] and _equal(metric["value"], mse), "开发validation正式指标不一致")
    _require(metric["unit"] == "squared_decimal_price_change" and metric["status"] == "computed" and metric["sample_size"] == count, "开发validation指标单位或样本数不符")
    sessions = [_day(row["observation_session"]) for row in model["tables"]["validation_predictions"]]
    _require(_day(metric["sample_start"]) == min(sessions) and _day(metric["sample_end"]) == max(sessions), "开发validation日期范围不符")
