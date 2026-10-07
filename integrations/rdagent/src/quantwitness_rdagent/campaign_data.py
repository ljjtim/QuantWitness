"""读取研究会话获准使用的开发预测，不向提案器暴露最终评价。"""
from datetime import date, datetime
import json
import math
from pathlib import Path

COLUMNS = (
    "candidate_id", "fold_id", "sample_id", "entity_id", "observation_session",
    "prediction", "actual", "label_available_time", "decision_time",
    "feature_available_time", "label_start_time", "label_end_time", "stage",
    "horizon_sessions", "score_semantics",
)


def _time(value, name):
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    except (TypeError, ValueError):
        raise ValueError("campaign.invalid_time:" + name) from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("campaign.timezone_required:" + name)
    return parsed


def _day(value, name):
    try:
        parsed = value if isinstance(value, date) and not isinstance(value, datetime) else date.fromisoformat(value)
    except (TypeError, ValueError):
        raise ValueError("campaign.invalid_date:" + name) from None
    return parsed


def _positive(value, name):
    if type(value) is not int or value < 1:
        raise ValueError("campaign.positive_integer_required:" + name)
    return value


def _load_verified(source):
    from research_pipeline.evidence import load_verified_result_context

    return load_verified_result_context(
        source["verification_result"], result_store=source["result_store"],
        additional_table_ids=("validation_predictions", "study_design"),
    )


def _formal_rows(source, budget):
    if source["table_id"] != "validation_predictions" or source["design_table_id"] != "study_design":
        raise ValueError("campaign.only_validation_predictions_and_study_design")
    context = _load_verified(source)
    verdict = context.verification
    if verdict.status != "pass":
        raise ValueError("campaign.source_verification_not_passed")
    bundle = context.snapshot.bundle
    if source["result_id"] != bundle.result_id or verdict.result_reference.result_id != bundle.result_id:
        raise ValueError("campaign.source_result_mismatch")
    tables = {}
    for name in ("validation_predictions", "study_design"):
        matches = [table for table in bundle.tables if table.table_id == name]
        if len(matches) != 1:
            raise ValueError("campaign.source_table_not_unique:" + name)
        tables[name] = matches[0]
    prediction_schema = tables["validation_predictions"].schema_id
    names = context.snapshot.table_schema(prediction_schema).names
    if not set(COLUMNS) <= set(names):
        raise ValueError("campaign.prediction_columns_missing")
    design_rows = []
    for batch in context.snapshot.iter_table_batches(
        tables["study_design"].schema_id, columns=("design_json",), batch_size=2
    ):
        if batch.nbytes * 8 > budget["memory_bytes"]:
            raise ValueError("campaign.memory_budget_exceeded")
        design_rows.extend(batch.to_pylist())
        if len(design_rows) > 1:
            raise ValueError("campaign.study_design_not_singleton")
    if len(design_rows) != 1:
        raise ValueError("campaign.study_design_not_singleton")
    design = json.loads(design_rows[0]["design_json"])

    def rows():
        batch_size = max(1, min(8192, budget["max_rows"] + 1, budget["memory_bytes"] // 8192))
        for batch in context.snapshot.iter_table_batches(
            prediction_schema, columns=COLUMNS, batch_size=batch_size
        ):
            if batch.nbytes * 8 > budget["memory_bytes"]:
                raise ValueError("campaign.memory_budget_exceeded")
            yield from batch.to_pylist()

    provenance = {
        "kind": "verified_result", "result_reference": verdict.result_reference.to_dict(),
        "result_id": bundle.result_id, "verification_hash": verdict.verification_hash,
        "table_id": "validation_predictions", "schema_id": prediction_schema,
    }
    return rows(), design, provenance


def load_development(payload):
    """返回JSON可序列化开发行和来源引用；不读取模型或启动研究。"""
    source, development, budget = payload["source"], payload["development"], payload["budget"]
    maximum = _positive(budget["max_rows"], "max_rows")
    memory = _positive(budget["memory_bytes"], "memory_bytes")
    start, end = (_day(development[key], key) for key in ("start", "end"))
    if start > end:
        raise ValueError("campaign.development_window_invalid")
    as_of = _time(development["as_of"], "as_of")
    horizon = _positive(development["horizon_sessions"], "horizon_sessions")
    folds = development["fold_ids"]
    if (not isinstance(folds, list) or not folds or any(not isinstance(item, str) or not item for item in folds)
            or len(set(folds)) != len(folds)):
        raise ValueError("campaign.fold_ids_invalid")
    if source["kind"] == "verified_result":
        iterator, design, provenance = _formal_rows(source, budget)
    elif source["kind"] == "synthetic":
        path = Path(source["path"])
        if path.stat().st_size * 8 > memory:
            raise ValueError("campaign.memory_budget_exceeded")
        fixture = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(fixture, dict) or fixture.get("fixture") != "synthetic":
            raise ValueError("campaign.synthetic_fixture_marker_required")
        iterator, design = fixture["rows"], fixture["design"]
        if not isinstance(iterator, list):
            raise ValueError("campaign.synthetic_rows_invalid")
        provenance = {"kind": "synthetic", "fixture": "synthetic", "path": str(path.resolve())}
    else:
        raise ValueError("campaign.source_kind_invalid")
    holdout = _time(design["holdout_start"], "holdout_start")
    if not as_of < holdout or end >= holdout.date():
        raise ValueError("campaign.development_reaches_holdout")
    rows, used, observed_folds, keys = [], 0, set(), set()
    for raw in iterator:
        if not isinstance(raw, dict) or not set(COLUMNS) <= set(raw):
            raise ValueError("campaign.prediction_columns_missing")
        if raw["stage"] != "validation":
            raise ValueError("campaign.non_validation_row")
        day = _day(raw["observation_session"], "observation_session")
        if (not start <= day <= end or raw["fold_id"] not in folds
                or raw["horizon_sessions"] != horizon):
            continue
        if raw["score_semantics"] != "raw_return_prediction":
            raise ValueError("campaign.raw_return_prediction_required")
        for name in ("candidate_id", "fold_id", "sample_id", "entity_id"):
            if not isinstance(raw[name], str) or not raw[name]:
                raise ValueError("campaign.prediction_identity_invalid:" + name)
        if type(raw["horizon_sessions"]) is not int:
            raise ValueError("campaign.horizon_sessions_invalid")
        for name in ("prediction", "actual"):
            if isinstance(raw[name], bool) or not isinstance(raw[name], (int, float)) or not math.isfinite(raw[name]):
                raise ValueError("campaign.finite_prediction_required:" + name)
        times = {key: _time(raw[key], key) for key in (
            "feature_available_time", "decision_time", "label_start_time", "label_end_time", "label_available_time"
        )}
        if not (times["feature_available_time"] <= times["decision_time"] <= times["label_start_time"]
                <= times["label_end_time"] <= times["label_available_time"] <= as_of
                and times["label_end_time"] < holdout):
            raise ValueError("campaign.development_time_order_invalid")
        key = (raw["candidate_id"], raw["fold_id"], raw["sample_id"])
        if key in keys:
            raise ValueError("campaign.duplicate_prediction")
        keys.add(key)
        row = {name: raw[name] for name in COLUMNS}
        row["observation_session"] = day.isoformat()
        row.update({name: value.isoformat() for name, value in times.items()})
        used += len(json.dumps(row, ensure_ascii=False, allow_nan=False).encode("utf-8"))
        if len(rows) >= maximum or used * 8 > memory:
            raise ValueError("campaign.development_budget_exceeded")
        rows.append(row)
        observed_folds.add(row["fold_id"])
    if not rows or observed_folds != set(folds):
        raise ValueError("campaign.development_fold_empty")
    return {"rows": rows, "provenance": provenance, "holdout_start": holdout.isoformat()}
