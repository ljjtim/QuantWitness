"""冻结分钟市场表的 Arrow 列定义、读取与来源绑定。"""
from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from research_pipeline.platform.arrow_rows import record_batch_rows
from .minute_rule_snapshots import MinuteRuleSnapshotError
from .session_calendar import SessionPolicyBundle


DAILY_REFERENCE_FILE = "daily-reference.parquet"
_DAILY_SECTIONS = {"instrument_lifecycle": "lifecycle", "price_limit": "prices", "suspension": "suspension"}
_DAILY_RULE_IDS = {f"rule.{market}.{kind}.v1": section for market in ("cn_stock", "cn_fund")
                   for kind, section in _DAILY_SECTIONS.items()}
_DAILY_PRICE_FIELDS = {
    name: pa.int64() for name in (
        "price_scale", "price_tick_units", "high_limit_units", "low_limit_units",
        "reference_previous_close_units", "reference_price_units", "price_limit_ratio_ppm",
        "price_limit_up_ratio_ppm", "price_limit_down_ratio_ppm",
    )
} | {
    name: pa.string() for name in (
        "price_limit_mode", "reference_price_kind", "reference_price_available_at",
    )
}
_DAILY_FIELDS = {
    "lifecycle": {"is_st": pa.bool_()} | {
        name: pa.string() for name in ("stock_board", "listing_phase", "trading_status")
    },
    "prices": _DAILY_PRICE_FIELDS,
    "suspension": {"paused": pa.bool_()},
}
DAILY_PARAMETER_COLUMNS = {name for section in _DAILY_FIELDS.values() for name in section}


_INSTRUMENT = pa.struct([
    (name, pa.string()) for name in (
        "instrument_id", "asset_class", "instrument_class", "exchange",
        "product_class", "role", "metadata_hash", "contract_version",
    )
])
_SEGMENT = pa.struct([
    ("segment_id", pa.string()), ("start_day_offset", pa.int64()),
    ("start_time", pa.string()), ("end_day_offset", pa.int64()),
    ("end_time", pa.string()), ("phase", pa.string()), ("bar_eligible", pa.bool_()),
    ("bucket_anchor_day_offset", pa.int64()), ("bucket_anchor_time", pa.string()),
    ("contract_version", pa.string()),
])
_SCHEMAS = {
    "instruments.parquet": pa.schema([("instrument", _INSTRUMENT), ("in_rules", pa.bool_())]),
    "sources.parquet": pa.schema([(name, pa.string()) for name in (
        "source_id", "source_kind", "locator", "published_on", "claim_ceiling", "contract_version",
    )]),
    "segments.parquet": pa.schema([("segments", pa.list_(_SEGMENT))]),
    "policies.parquet": pa.schema([
        ("policy_id", pa.string()), ("revision", pa.int64()), ("instrument_index", pa.int64()),
        ("effective_from", pa.string()), ("effective_to", pa.string()),
        ("trading_dates", pa.list_(pa.string())), ("segment_index", pa.int64()),
        ("evidence_hashes", pa.list_(pa.struct([("source_id", pa.string()), ("content_hash", pa.string())]))),
        ("auction_bar_policy", pa.string()), ("previous_policy_hash", pa.string()),
        ("claim_ceiling", pa.string()), ("contract_version", pa.string()), ("available_at", pa.string()),
    ]),
    "parameters.parquet": pa.schema([("parameters", pa.string())]),
    "rules.parquet": pa.schema([
        ("rule_id", pa.string()), ("revision", pa.int64()), ("instrument_id", pa.string()),
        ("asset_class", pa.string()), ("status", pa.string()),
        ("effective_from", pa.string()), ("effective_to", pa.string()), ("available_at", pa.string()),
        ("source_ids", pa.list_(pa.string())), ("parameter_index", pa.int64()),
        ("unsupported_reason", pa.string()), ("contract_version", pa.string()),
    ]),
}
_DAILY_SHARED = {"instrument_id", "asset_class", "effective_from", "effective_to"}
_DAILY_TIMELINE = [field for field in _SCHEMAS["rules.parquet"] if field.name not in _DAILY_SHARED]
_SCHEMAS[DAILY_REFERENCE_FILE] = pa.schema(
    [("instrument_id", pa.string()), ("asset_class", pa.string()), ("trading_date", pa.string())]
    + [(f"{section}_{field.name}", field.type) for section in _DAILY_FIELDS for field in _DAILY_TIMELINE]
    + [(f"{section}_{name}", dtype) for section, fields in _DAILY_FIELDS.items() for name, dtype in fields.items()]
)
DAILY_REFERENCE_SCHEMA = _SCHEMAS[DAILY_REFERENCE_FILE]


def _binding(meta: dict) -> tuple[tuple[str, str], ...]:
    binding = meta["capability_binding"]
    if not isinstance(binding, Mapping) or any(
        not isinstance(key, str) or not isinstance(value, str) for key, value in binding.items()
    ):
        raise MinuteRuleSnapshotError("分钟输入 capability_binding 无效")
    return tuple(sorted(binding.items()))

def _directory(root: Path, reference: dict[str, object]) -> Path:
    return root / "minute-scopes" / reference["name"]

def _rows(directory: Path, filename: str, *, as_dict=True):
    try:
        parquet = pq.ParquetFile(directory / filename)
        if not parquet.schema_arrow.equals(_SCHEMAS[filename], check_metadata=False):
            raise MinuteRuleSnapshotError(f"分钟输入 {filename} 列或类型不符")
        for batch in parquet.iter_batches(batch_size=2048):
            yield from record_batch_rows(batch, as_dict=as_dict)
    except (pa.ArrowException, OSError) as exc:
        raise MinuteRuleSnapshotError(f"分钟输入 {filename} 无法读取") from exc

def _require_binding(calendar: SessionPolicyBundle, rules) -> None:
    if (dict(calendar.capability_binding)["minute_capability_manifest_hash"]
            != dict(rules.capability_binding)["minute_capability_manifest_hash"]):
        raise MinuteRuleSnapshotError("分钟输入规则与日历能力来源不一致")
    sources = tuple(item for item in rules.sources if item.source_kind == "repository_snapshot")
    if len(sources) != 1 or not sources[0].locator.endswith("#" + calendar.bundle_hash):
        raise MinuteRuleSnapshotError("分钟输入规则未绑定完整日历来源")
