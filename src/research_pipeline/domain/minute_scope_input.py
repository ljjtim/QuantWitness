"""分钟完整来源的固定列式存储；沿用日历与规则的原有身份。"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from functools import lru_cache
import json
from pathlib import Path
import re
import shutil
from types import MappingProxyType

import pyarrow as pa
import pyarrow.parquet as pq

from .minute_rule_snapshots import (
    MinuteRuleSnapshot, MinuteRuleSnapshotBundle, MinuteRuleSnapshotError,
    MinuteRuleSource, _date as _rule_date, _datetime as _rule_datetime,
)
from .session_calendar import (
    SessionInstrumentMetadata, SessionPolicyBundle, SessionPolicyRevision,
    SessionSegmentTemplate,
)


MINUTE_SCOPE_INPUT_VERSION = "minute-scope-input-v1"
_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]*\Z")
_HASH = re.compile(r"[0-9a-f]{64}\Z")
_RESERVED_NAMES = {"CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(1, 10)), *(f"LPT{i}" for i in range(1, 10))}
_REFERENCE_KEYS = {"contract_version", "name", "calendar_hash", "rules_hash"}
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
_FILES = {"manifest.json", *_SCHEMAS}
_CALENDAR_KEYS = {"bundle_id", "capability_binding", "inventory_evidence_hash", "contract_version"}
_RULE_KEYS = {"bundle_id", "capability_binding", "contract_version"}


@dataclass(frozen=True)
class MinuteScopeInput:
    reference: Mapping[str, object]
    root: Path
    calendar: SessionPolicyBundle
    rules: MinuteRuleSnapshotBundle

    def __post_init__(self) -> None:
        object.__setattr__(self, "reference", MappingProxyType(validate_minute_scope_reference(self.reference)))


def validate_minute_scope_reference(reference: object) -> dict[str, object]:
    """只检查小引用，不访问来源文件。"""
    if not isinstance(reference, Mapping) or set(reference) != _REFERENCE_KEYS:
        raise MinuteRuleSnapshotError("分钟输入引用字段不符")
    if reference["contract_version"] != MINUTE_SCOPE_INPUT_VERSION:
        raise MinuteRuleSnapshotError("分钟输入合同版本不受支持")
    name = reference["name"]
    if not isinstance(name, str) or _NAME.fullmatch(name) is None or name.upper() in _RESERVED_NAMES:
        raise MinuteRuleSnapshotError("分钟输入 name 必须是普通标识")
    for key in ("calendar_hash", "rules_hash"):
        if not isinstance(reference[key], str) or _HASH.fullmatch(reference[key]) is None:
            raise MinuteRuleSnapshotError(f"分钟输入 {key} 无效")
    return dict(reference)


def _directory(root: Path, reference: dict[str, object]) -> Path:
    return root / "minute-scopes" / reference["name"]


def _manifest(root: Path, reference: dict[str, object]) -> dict:
    directory = _directory(root, reference)
    if not directory.is_dir() or {item.name for item in directory.iterdir()} != _FILES:
        raise MinuteRuleSnapshotError("分钟输入存在缺文件或未知文件")
    if any(not (directory / name).is_file() for name in _FILES):
        raise MinuteRuleSnapshotError("分钟输入固定布局必须是文件")
    try:
        manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    except (ValueError, OSError) as exc:
        raise MinuteRuleSnapshotError("分钟输入 manifest 无效") from exc
    if not isinstance(manifest, dict) or set(manifest) != {"reference", "calendar", "rules"}:
        raise MinuteRuleSnapshotError("分钟输入 manifest 字段不符")
    if validate_minute_scope_reference(manifest["reference"]) != reference:
        raise MinuteRuleSnapshotError("分钟输入 descriptor 与 manifest 来源不一致")
    for key, expected in (("calendar", _CALENDAR_KEYS), ("rules", _RULE_KEYS)):
        if not isinstance(manifest[key], dict) or set(manifest[key]) != expected:
            raise MinuteRuleSnapshotError("分钟输入 bundle 元数据字段不符")
    return manifest


def _require_binding(calendar: SessionPolicyBundle, rules: MinuteRuleSnapshotBundle) -> None:
    if (dict(calendar.capability_binding)["minute_capability_manifest_hash"]
            != dict(rules.capability_binding)["minute_capability_manifest_hash"]):
        raise MinuteRuleSnapshotError("分钟输入规则与日历能力来源不一致")
    sources = tuple(item for item in rules.sources if item.source_kind == "repository_snapshot")
    if len(sources) != 1 or not sources[0].locator.endswith("#" + calendar.bundle_hash):
        raise MinuteRuleSnapshotError("分钟输入规则未绑定完整日历来源")


def _rows(directory: Path, filename: str):
    try:
        parquet = pq.ParquetFile(directory / filename)
        if not parquet.schema_arrow.equals(_SCHEMAS[filename], check_metadata=False):
            raise MinuteRuleSnapshotError(f"分钟输入 {filename} 列或类型不符")
        for batch in parquet.iter_batches(batch_size=2048):
            yield from batch.to_pylist()
    except (pa.ArrowException, OSError) as exc:
        raise MinuteRuleSnapshotError(f"分钟输入 {filename} 无法读取") from exc


def _index(items, value):
    if type(value) is not int or not 0 <= value < len(items):
        raise MinuteRuleSnapshotError("分钟输入模板或证券索引无效")
    return items[value]


def load_minute_scope_input(root, reference) -> MinuteScopeInput:
    """逐批恢复 typed bundle；完整身份由既有构造器复核，不选取版本。"""
    reference = validate_minute_scope_reference(reference)
    root = Path(root)
    manifest = _manifest(root, reference)
    directory = _directory(root, reference)
    instruments = []
    rule_instruments = []
    for row in _rows(directory, "instruments.parquet"):
        instrument = SessionInstrumentMetadata.from_dict(row["instrument"])
        if type(row["in_rules"]) is not bool:
            raise MinuteRuleSnapshotError("分钟输入证券归属无效")
        instruments.append(instrument)
        if row["in_rules"]:
            rule_instruments.append(instrument)
    segments = tuple(
        tuple(SessionSegmentTemplate.from_dict(item) for item in row["segments"])
        for row in _rows(directory, "segments.parquet")
    )
    dates = {}
    evidence = {}
    policies = []
    for row in _rows(directory, "policies.parquet"):
        day_key = tuple(row["trading_dates"])
        if day_key not in dates:
            dates[day_key] = tuple(date.fromisoformat(day) for day in day_key)
        evidence_key = tuple((item["source_id"], item["content_hash"]) for item in row["evidence_hashes"])
        hashes = evidence.setdefault(evidence_key, evidence_key)
        policies.append(SessionPolicyRevision(
            policy_id=row["policy_id"], revision=row["revision"],
            instrument=_index(instruments, row["instrument_index"]),
            effective_from=date.fromisoformat(row["effective_from"]),
            effective_to=date.fromisoformat(row["effective_to"]),
            trading_dates=dates[day_key], segments=_index(segments, row["segment_index"]),
            evidence_hashes=hashes, auction_bar_policy=row["auction_bar_policy"],
            previous_policy_hash=row["previous_policy_hash"], claim_ceiling=row["claim_ceiling"],
            contract_version=row["contract_version"],
            available_at=None if row["available_at"] is None else datetime.fromisoformat(row["available_at"]),
        ))
    meta = manifest["calendar"]
    calendar = SessionPolicyBundle(
        bundle_id=meta["bundle_id"], capability_binding=_binding(meta),
        inventory_evidence_hash=meta["inventory_evidence_hash"], policies=tuple(policies),
        bundle_hash=reference["calendar_hash"], contract_version=meta["contract_version"],
    )
    parameters = []
    for row in _rows(directory, "parameters.parquet"):
        payload = json.loads(row["parameters"])
        if not isinstance(payload, dict):
            raise MinuteRuleSnapshotError("分钟输入参数模板必须是对象")
        parameters.append(tuple(sorted(payload.items())))
    sources = tuple(MinuteRuleSource.from_dict(row) for row in _rows(directory, "sources.parquet"))
    snapshots = []
    # 同次加载按原始文本复用解析，保留不同时区表示和首次解析的拒绝行为。
    parse_rule_date = lru_cache(maxsize=4096)(_rule_date)
    parse_rule_datetime = lru_cache(maxsize=4096)(_rule_datetime)
    for row in _rows(directory, "rules.parquet"):
        snapshots.append(MinuteRuleSnapshot(
            rule_id=row["rule_id"], revision=row["revision"], instrument_id=row["instrument_id"],
            asset_class=row["asset_class"], status=row["status"],
            effective_from=parse_rule_date(row["effective_from"], "effective_from"),
            effective_to=parse_rule_date(row["effective_to"], "effective_to"),
            available_at=None if row["available_at"] is None else parse_rule_datetime(row["available_at"], "available_at"),
            source_ids=tuple(row["source_ids"]), parameters=_index(parameters, row["parameter_index"]),
            unsupported_reason=row["unsupported_reason"], contract_version=row["contract_version"],
        ))
    meta = manifest["rules"]
    rules = MinuteRuleSnapshotBundle(
        bundle_id=meta["bundle_id"], capability_binding=_binding(meta), sources=sources,
        instruments=tuple(rule_instruments), rules=tuple(snapshots),
        bundle_hash=reference["rules_hash"], contract_version=meta["contract_version"],
    )
    _require_binding(calendar, rules)
    return MinuteScopeInput(reference, root, calendar, rules)


def _binding(meta: dict) -> tuple[tuple[str, str], ...]:
    binding = meta["capability_binding"]
    if not isinstance(binding, Mapping) or any(
        not isinstance(key, str) or not isinstance(value, str) for key, value in binding.items()
    ):
        raise MinuteRuleSnapshotError("分钟输入 capability_binding 无效")
    return tuple(sorted(binding.items()))


def _write_rows(directory: Path, filename: str, rows) -> None:
    schema = _SCHEMAS[filename]
    with pq.ParquetWriter(directory / filename, schema, compression="zstd") as writer:
        batch = []
        for row in rows:
            batch.append(row)
            if len(batch) == 2048:
                writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                batch = []
        if batch:
            writer.write_table(pa.Table.from_pylist(batch, schema=schema))


def write_minute_scope_input(root, *, name, calendar: SessionPolicyBundle,
                             rules: MinuteRuleSnapshotBundle) -> dict:
    """写入完整版本流，已有同名输入只接受相同来源。"""
    reference = validate_minute_scope_reference({
        "contract_version": MINUTE_SCOPE_INPUT_VERSION, "name": name,
        "calendar_hash": calendar.bundle_hash, "rules_hash": rules.bundle_hash,
    })
    _require_binding(calendar, rules)
    root = Path(root)
    directory = _directory(root, reference)
    if directory.exists():
        load_minute_scope_input(root, reference)
        return reference
    directory.mkdir(parents=True)
    instruments = list(rules.instruments)
    instrument_indices = {item: index for index, item in enumerate(instruments)}
    templates = []
    template_indices = {}

    def policy_rows():
        for policy in calendar.policies:
            if policy.instrument not in instrument_indices:
                instrument_indices[policy.instrument] = len(instruments)
                instruments.append(policy.instrument)
            if policy.segments not in template_indices:
                template_indices[policy.segments] = len(templates)
                templates.append(policy.segments)
            # 单条政策转字典，不构造整包副本。
            row = policy.to_dict()
            del row["instrument"], row["segments"]
            row.update(instrument_index=instrument_indices[policy.instrument],
                       segment_index=template_indices[policy.segments])
            row.setdefault("available_at", None)
            yield row

    _write_rows(directory, "policies.parquet", policy_rows())
    _write_rows(directory, "instruments.parquet", (
        {"instrument": item.to_dict(), "in_rules": index < len(rules.instruments)}
        for index, item in enumerate(instruments)
    ))
    _write_rows(directory, "segments.parquet", (
        {"segments": [item.to_dict() for item in template]} for template in templates
    ))
    _write_rows(directory, "sources.parquet", (item.to_dict() for item in rules.sources))
    parameter_values = []
    parameter_indices = {}

    def rule_rows():
        for rule in rules.rules:
            row = rule.to_dict()
            # JSON 保留 null、bool、整数、浮点和嵌套列表/对象，不用 Arrow 推断异构参数。
            encoded = json.dumps(row.pop("parameters"), ensure_ascii=False, sort_keys=True,
                                 separators=(",", ":"), allow_nan=False)
            if encoded not in parameter_indices:
                parameter_indices[encoded] = len(parameter_values)
                parameter_values.append(encoded)
            row["parameter_index"] = parameter_indices[encoded]
            yield row

    _write_rows(directory, "rules.parquet", rule_rows())
    _write_rows(directory, "parameters.parquet", ({"parameters": item} for item in parameter_values))
    manifest = {
        "reference": reference,
        "calendar": {"bundle_id": calendar.bundle_id, "capability_binding": dict(calendar.capability_binding),
                     "inventory_evidence_hash": calendar.inventory_evidence_hash,
                     "contract_version": calendar.contract_version},
        "rules": {"bundle_id": rules.bundle_id, "capability_binding": dict(rules.capability_binding),
                  "contract_version": rules.contract_version},
    }
    (directory / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return reference


def copy_minute_scope_input(source: MinuteScopeInput, root) -> dict:
    """复制已装载来源；固定布局复核不再计算源文件摘要。"""
    reference = validate_minute_scope_reference(source.reference)
    if (source.calendar.bundle_hash != reference["calendar_hash"]
            or source.rules.bundle_hash != reference["rules_hash"]):
        raise MinuteRuleSnapshotError("分钟输入对象与 descriptor 来源不一致")
    _require_binding(source.calendar, source.rules)
    _manifest(Path(source.root), reference)
    root = Path(root)
    target = _directory(root, reference)
    if target.exists():
        load_minute_scope_input(root, reference)
        return reference
    target.mkdir(parents=True)
    for filename in sorted(_FILES):
        shutil.copyfile(_directory(Path(source.root), reference) / filename, target / filename)
    return reference
