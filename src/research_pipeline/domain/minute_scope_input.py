"""分钟完整来源的固定列式存储；沿用日历与规则的原有身份。"""
from __future__ import annotations

from collections.abc import Mapping
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import date, datetime, timezone
from functools import cached_property, lru_cache
import hashlib
from heapq import merge
from itertools import groupby
import json
from pathlib import Path
import re
from tempfile import TemporaryDirectory
from types import MappingProxyType

import pyarrow as pa
import pyarrow.parquet as pq

from research_pipeline.platform.arrow_rows import record_batch_rows
from research_pipeline.platform import typed_canonical_hash
from research_pipeline.platform.immutable_files import freeze_file, link_readonly_file, remove_tree

from .minute_scope_tables import (
    DAILY_PARAMETER_COLUMNS, DAILY_REFERENCE_FILE, DAILY_REFERENCE_SCHEMA, _DAILY_FIELDS,
    _DAILY_RULE_IDS, _DAILY_TIMELINE, _SCHEMAS, _binding, _directory, _require_binding, _rows,
)
from .minute_scope_tables import _DAILY_SHARED
from .minute_rule_snapshots import (
    MINUTE_RULE_VERSION, PROJECT_RULE_BUNDLE_VERSION,
    MinuteRuleSnapshot, MinuteRuleSnapshotBundle, MinuteRuleSnapshotError,
    MinuteRuleSource, _date as _rule_date, _datetime as _rule_datetime,
)
from .session_calendar import (
    SessionInstrumentMetadata, SessionPolicyBundle, SessionPolicyRevision,
    SessionSegmentTemplate,
)


MINUTE_SCOPE_INPUT_VERSION = "minute-scope-input-v1"
_DAILY_REFERENCE_LAYOUT = "minute-scope-daily-reference-v2"
_COLUMNAR_RULE_IDENTITY = "minute-scope-columnar-rule-identity-v1"
_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]*\Z")
_HASH = re.compile(r"[0-9a-f]{64}\Z")
_RESERVED_NAMES = {"CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(1, 10)), *(f"LPT{i}" for i in range(1, 10))}
_REFERENCE_KEYS = {"contract_version", "name", "calendar_hash", "rules_hash"}
_COVERAGE_DIGEST = "rule_coverage_digest"
_CALENDAR_FILES_HASH = "calendar_files_hash"
_CALENDAR_FILES = ("instruments.parquet", "segments.parquet", "policies.parquet")
_FILES = {"manifest.json", *_SCHEMAS}
_CALENDAR_KEYS = {"bundle_id", "capability_binding", "inventory_evidence_hash", "contract_version"}
_RULE_KEYS = {"bundle_id", "capability_binding", "contract_version"}


@dataclass(frozen=True)
class MinuteScopeInput:
    reference: Mapping[str, object]
    root: Path
    _metadata: dict
    files: Mapping[str, str]
    _admitted_calendar: tuple | None = None
    _frozen: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "reference", MappingProxyType(validate_minute_scope_reference(self.reference)))
        object.__setattr__(self, "files", MappingProxyType(dict(self.files)))

    @cached_property
    def _calendar_parts(self) -> tuple:
        if self._admitted_calendar is not None:
            return self._admitted_calendar
        return _load_scope_calendar(self.root, self.reference, self._metadata,
                                    verified_columnar=self._frozen or _calendar_files_bound(self.reference))

    @property
    def calendar(self) -> SessionPolicyBundle:
        return self._calendar_parts[0]

    @property
    def calendar_metadata(self) -> Mapping[str, object]:
        return self._metadata["calendar"]

    @property
    def rule_metadata(self) -> Mapping[str, object]:
        return self._metadata["rules"]

    def require_coverage(self, **request):
        from .minute_request_scope import require_minute_request_coverage

        if self.has_admitted_coverage(**request):
            return
        if self._metadata.get("rule_identity") == _COLUMNAR_RULE_IDENTITY:
            from .minute_scope_columnar import validate_columnar_rules

            validate_columnar_rules(self, [request])
        else:
            require_minute_request_coverage(self.rules, self.calendar, **request)

    @cached_property
    def rules(self) -> MinuteRuleSnapshotBundle:
        if "rule_identity" in self._metadata:
            raise MinuteRuleSnapshotError("列式现货规则通过冻结规则表消费")
        return _load_scope_rules(self.root, self.reference, self._metadata, *self._calendar_parts)

    def has_admitted_coverage(self, *, instruments, start_at, end_at, stock_raw=False) -> bool:
        scopes = self._metadata.get("admitted_rule_coverage", ())
        if not scopes:
            return False
        request = _coverage_payload(instruments, start_at, end_at, stock_raw=stock_raw)
        if "rule_identity" not in self._metadata:
            return request in scopes
        requested_codes = set(request["instruments"])
        return any(
            request.get("stock_raw") == admitted.get("stock_raw")
            and datetime.fromisoformat(admitted["start_at"]) <= start_at
            and end_at <= datetime.fromisoformat(admitted["end_at"])
            and requested_codes.issubset(admitted["instruments"])
            for admitted in scopes
        )


def validate_minute_scope_reference(reference: object) -> dict[str, object]:
    """只检查小引用，不访问来源文件。"""
    if not isinstance(reference, Mapping) or (
            set(reference) - {_COVERAGE_DIGEST, _CALENDAR_FILES_HASH} != _REFERENCE_KEYS
    ):
        raise MinuteRuleSnapshotError("分钟输入引用字段不符")
    if reference["contract_version"] != MINUTE_SCOPE_INPUT_VERSION:
        raise MinuteRuleSnapshotError("分钟输入合同版本不受支持")
    name = reference["name"]
    if not isinstance(name, str) or _NAME.fullmatch(name) is None or name.upper() in _RESERVED_NAMES:
        raise MinuteRuleSnapshotError("分钟输入 name 必须是普通标识")
    for key in (set(reference) - {"contract_version", "name"}):
        if not isinstance(reference[key], str) or _HASH.fullmatch(reference[key]) is None:
            raise MinuteRuleSnapshotError(f"分钟输入 {key} 无效")
    return dict(reference)


def _calendar_files_bound(reference):
    return _CALENDAR_FILES_HASH in reference or _COVERAGE_DIGEST in reference


def _manifest(root: Path, reference: Mapping[str, object], *, expected_digest: str | None = None) -> dict:
    directory = _directory(root, reference)
    actual_files = {item.name for item in directory.iterdir()} if directory.is_dir() else set()
    if actual_files not in (_FILES, _FILES - {DAILY_REFERENCE_FILE}):
        raise MinuteRuleSnapshotError("分钟输入存在缺文件或未知文件")
    if any(not (directory / name).is_file() for name in actual_files):
        raise MinuteRuleSnapshotError("分钟输入固定布局必须是文件")
    try:
        content = (directory / "manifest.json").read_bytes()
        manifest = json.loads(content)
    except (ValueError, OSError) as exc:
        raise MinuteRuleSnapshotError("分钟输入 manifest 无效") from exc
    if expected_digest is not None and hashlib.sha256(content).hexdigest() != expected_digest:
        raise MinuteRuleSnapshotError("冻结分钟输入 manifest 文件摘要不一致")
    base = {"reference", "calendar", "rules"}
    if not isinstance(manifest, dict) or set(manifest) - {"files", "rules_layout", "admitted_rule_coverage", "rule_identity"} != base:
        raise MinuteRuleSnapshotError("分钟输入 manifest 字段不符")
    if ("rules_layout" in manifest and manifest["rules_layout"] != _DAILY_REFERENCE_LAYOUT
            or (manifest.get("rules_layout") == _DAILY_REFERENCE_LAYOUT) != (DAILY_REFERENCE_FILE in actual_files)):
        raise MinuteRuleSnapshotError("分钟行情参考物理布局与文件不符")
    if "files" in manifest:
        files = manifest["files"]
        if not isinstance(files, dict) or set(files) != actual_files - {"manifest.json"} or any(
            not isinstance(digest, str) or _HASH.fullmatch(digest) is None for digest in files.values()
        ):
            raise MinuteRuleSnapshotError("冻结分钟输入 Parquet 摘要字段不符")
    for key, expected in (("calendar", _CALENDAR_KEYS), ("rules", _RULE_KEYS)):
        if not isinstance(manifest[key], dict) or set(manifest[key]) != expected:
            raise MinuteRuleSnapshotError("分钟输入 bundle 元数据字段不符")
    if _CALENDAR_FILES_HASH in reference and (
            "files" not in manifest or reference[_CALENDAR_FILES_HASH] != _columnar_calendar_hash(manifest)):
        raise MinuteRuleSnapshotError("列式日历文件身份不一致")
    if "rule_identity" in manifest:
        if (manifest["rule_identity"] != _COLUMNAR_RULE_IDENTITY
                or manifest.get("rules_layout") != _DAILY_REFERENCE_LAYOUT
                or "files" not in manifest):
            raise MinuteRuleSnapshotError("列式规则身份合同无效")
        if reference["rules_hash"] != _columnar_rules_hash(manifest):
            raise MinuteRuleSnapshotError("列式规则文件身份不一致")
    if "admitted_rule_coverage" in manifest:
        if "files" not in manifest or "rules_layout" not in manifest:
            raise MinuteRuleSnapshotError("规则覆盖证明须绑定逐日列式文件")
        scopes = manifest["admitted_rule_coverage"]
        if not isinstance(scopes, list) or not scopes:
            raise MinuteRuleSnapshotError("规则覆盖证明无效")
        for item in scopes:
            if (not isinstance(item, Mapping)
                    or set(item) - {"stock_raw"} != {"instruments", "start_at", "end_at"}):
                raise MinuteRuleSnapshotError("规则覆盖证明字段无效")
            try:
                start, end = (datetime.fromisoformat(item[key]) for key in ("start_at", "end_at"))
            except (TypeError, ValueError) as exc:
                raise MinuteRuleSnapshotError("规则覆盖证明时间格式无效") from exc
            if item != _coverage_payload(item["instruments"], start, end, stock_raw=item.get("stock_raw", False)):
                raise MinuteRuleSnapshotError("规则覆盖证明范围无效")
        if reference.get(_COVERAGE_DIGEST) != _coverage_digest(reference, manifest):
            raise MinuteRuleSnapshotError("已声明规则覆盖证明与清单摘要不一致")
    elif _COVERAGE_DIGEST in reference:
        raise MinuteRuleSnapshotError("已声明规则覆盖证明缺失")
    if validate_minute_scope_reference(manifest["reference"]) != reference:
        raise MinuteRuleSnapshotError("分钟输入 descriptor 与 manifest 来源不一致")
    return manifest


def daily_reference_sections(rule_ids):
    return tuple(dict.fromkeys(_DAILY_RULE_IDS[rule_id] for rule_id in rule_ids if rule_id in _DAILY_RULE_IDS))


def daily_reference_batches(path, section, *, batch_size=32768):
    fields = _DAILY_FIELDS[section]
    shared = {"instrument_id": "instrument_id", "asset_class": "asset_class",
              "effective_from": "trading_date", "effective_to": "trading_date"}
    names = [field.name for field in _SCHEMAS["rules.parquet"]] + list(fields)
    layout = [shared[name] if name in shared else f"{section}_{name}" for name in names]
    with pq.ParquetFile(path) as parquet:
        if not parquet.schema_arrow.equals(_SCHEMAS[DAILY_REFERENCE_FILE], check_metadata=False):
            raise MinuteRuleSnapshotError("逐日参考表列或类型不符")
        for batch in parquet.iter_batches(batch_size=batch_size, columns=list(dict.fromkeys(layout))):
            rows = pa.RecordBatch.from_arrays([batch[name] for name in layout], names=names)
            yield rows.filter(rows["revision"].is_valid())


def _daily_rule_rows(directory, section):
    rows = (row for batch in daily_reference_batches(directory / DAILY_REFERENCE_FILE, section)
            for row in record_batch_rows(batch, as_dict=False))
    for _, instrument_rows in groupby(rows, key=lambda row: row[2]):
        yield from sorted(instrument_rows, key=lambda row: (row[0], row[1]))


def _index(items, value):
    if type(value) is not int or not 0 <= value < len(items):
        raise MinuteRuleSnapshotError("分钟输入模板或证券索引无效")
    return items[value]


def _calendar_policy_rows(directory):
    import pyarrow.compute as pc
    from .minute_scope_tables import _SCHEMAS
    from research_pipeline.platform.arrow_rows import record_batch_rows

    try:
        with pq.ParquetFile(directory / "policies.parquet") as parquet:
            if not parquet.schema_arrow.equals(_SCHEMAS["policies.parquet"], check_metadata=False):
                raise MinuteRuleSnapshotError("分钟输入 policies.parquet 列或类型不符")
            for batch in parquet.iter_batches(batch_size=2048):
                day_lists = batch["trading_dates"]
                encoded = pc.dictionary_encode(pc.list_flatten(day_lists))
                if day_lists.null_count or encoded.null_count:
                    raise MinuteRuleSnapshotError("分钟输入交易日不可为空值")
                unique_days = tuple(date.fromisoformat(day) for day in encoded.dictionary.to_pylist())
                offsets = day_lists.offsets.to_numpy()
                indices = encoded.indices.to_numpy()
                keys = {}
                for position, row in enumerate(record_batch_rows(batch.drop_columns(["trading_dates"]))):
                    begin, end = offsets[position:position + 2] - offsets[0]
                    key = indices[begin:end].tobytes()
                    days = keys.get(key)
                    if days is None:
                        days = tuple(unique_days[index] for index in indices[begin:end])
                        keys[key] = days
                    yield row, days
    except (ValueError, pa.ArrowException, OSError) as exc:
        raise MinuteRuleSnapshotError("分钟输入 policies.parquet 无法读取") from exc


def _load_scope_calendar(root, reference, manifest, *, verified_columnar=False):
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
    for row, ordered_days in _calendar_policy_rows(directory):
        # 每批日期字典只解码一次，跨批按相同日期序列共享 tuple。
        trading_days = dates.get(ordered_days)
        if trading_days is None:
            if any(first >= second for first, second in zip(ordered_days, ordered_days[1:])):
                raise MinuteRuleSnapshotError("分钟输入交易日必须严格递增")
            trading_days = dates.setdefault(ordered_days, ordered_days)
        evidence_key = tuple((item["source_id"], item["content_hash"]) for item in row["evidence_hashes"])
        hashes = evidence.setdefault(evidence_key, evidence_key)
        policies.append(SessionPolicyRevision(
            policy_id=row["policy_id"], revision=row["revision"],
            instrument=_index(instruments, row["instrument_index"]),
            effective_from=date.fromisoformat(row["effective_from"]),
            effective_to=date.fromisoformat(row["effective_to"]),
            trading_dates=trading_days, _ordered_trading_dates=True, segments=_index(segments, row["segment_index"]),
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
        _verified_columnar=verified_columnar,
    )
    return calendar, tuple(rule_instruments)


def load_minute_scope_calendar(root, reference) -> SessionPolicyBundle:
    """只恢复冻结来源的完整日历，不读取或解码规则表。"""
    reference = validate_minute_scope_reference(reference)
    root = Path(root)
    manifest = _manifest(root, reference)
    verified_columnar = _calendar_files_bound(reference)
    if "files" in manifest:
        directory = _directory(root, reference)
        if any(_digest_file(directory / name) != manifest["files"][name] for name in _CALENDAR_FILES):
            raise MinuteRuleSnapshotError("冻结分钟输入日历文件摘要不一致")
    calendar, _ = _load_scope_calendar(root, reference, manifest, verified_columnar=verified_columnar)
    return calendar


def load_minute_scope_input(root, reference, *, reuse_rule_coverage=False) -> MinuteScopeInput:
    """逐批恢复 typed bundle；完整身份由既有构造器复核，不选取版本。"""
    reference = validate_minute_scope_reference(reference)
    root = Path(root)
    manifest = _manifest(root, reference)
    # 摘要在语义准入读取前固定，复制时再核对实际复制的字节。
    files = _file_digests(_directory(root, reference))
    if "files" in manifest and manifest["files"] != files:
        raise MinuteRuleSnapshotError("冻结分钟输入 Parquet 文件摘要不一致")
    scope = MinuteScopeInput(reference, root, manifest, files)
    if "rule_identity" not in manifest and (not reuse_rule_coverage or not manifest.get("admitted_rule_coverage")):
        _ = scope.rules
    return scope


def _load_scope_rules(root, reference, manifest, calendar, rule_instruments) -> MinuteRuleSnapshotBundle:
    directory = _directory(root, reference)
    # 列式字典在解码后继续共享；仅共享不可变标量，嵌套载荷保持独立。
    share_text = lru_cache(maxsize=16384)(lambda value: value)

    @lru_cache(maxsize=65536)
    def scalar_pair(key, value_type, encoded):
        value = float.fromhex(encoded) if value_type is float else encoded
        return (share_text(key), share_text(value) if value_type is str else value)

    def parameter_pairs(payload):
        for key, value in sorted(payload.items()):
            if type(value) in (str, int, float, bool, type(None)):
                yield scalar_pair(key, type(value), value.hex() if type(value) is float else value)
            else:
                yield (share_text(key), value)

    parameters = []
    for row in _rows(directory, "parameters.parquet"):
        payload = json.loads(row["parameters"])
        if not isinstance(payload, dict):
            raise MinuteRuleSnapshotError("分钟输入参数模板必须是对象")
        parameters.append(tuple(parameter_pairs(payload)))
    sources = tuple(MinuteRuleSource.from_dict(row) for row in _rows(directory, "sources.parquet"))
    snapshots = []
    # 同次加载按原始文本复用解析，保留不同时区表示和首次解析的拒绝行为。
    parse_rule_date = lru_cache(maxsize=4096)(_rule_date)
    parse_rule_datetime = lru_cache(maxsize=4096)(_rule_datetime)
    share_sources = lru_cache(maxsize=16384)(lambda values: tuple(share_text(value) for value in values))
    rows = _rows(directory, "rules.parquet", as_dict=False)
    if manifest.get("rules_layout") == _DAILY_REFERENCE_LAYOUT:
        rows = merge(rows, *(_daily_rule_rows(directory, section) for section in _DAILY_FIELDS),
                     key=lambda row: (row[2], row[0], row[1]))
    for (rule_id, revision, instrument_id, asset_class, status, effective_from,
         effective_to, available_at, source_ids, parameter_index, unsupported_reason,
         contract_version, *daily_values) in rows:
        # _rows 已按固定 schema 校验列名、列顺序及类型。
        pairs = _index(parameters, parameter_index)
        if daily_values:
            daily = {name: value for name, value in zip(_DAILY_FIELDS[_DAILY_RULE_IDS[rule_id]], daily_values)
                     if value is not None}
            if daily:
                preserved = dict(pairs)
                if preserved.keys() & daily.keys():
                    raise MinuteRuleSnapshotError("逐日行情参数与制度模板字段重复")
                pairs = tuple(parameter_pairs(preserved | daily))
        snapshots.append(MinuteRuleSnapshot(
            rule_id=share_text(rule_id), revision=revision, instrument_id=share_text(instrument_id),
            asset_class=share_text(asset_class), status=share_text(status),
            effective_from=parse_rule_date(effective_from, "effective_from"),
            effective_to=parse_rule_date(effective_to, "effective_to"),
            available_at=None if available_at is None else parse_rule_datetime(available_at, "available_at"),
            source_ids=share_sources(tuple(source_ids)), parameters=pairs,
            unsupported_reason=share_text(unsupported_reason), contract_version=share_text(contract_version),
        ))
    meta = manifest["rules"]
    rules = MinuteRuleSnapshotBundle(
        bundle_id=meta["bundle_id"], capability_binding=_binding(meta), sources=sources,
        instruments=tuple(rule_instruments), rules=tuple(snapshots),
        bundle_hash=reference["rules_hash"], contract_version=meta["contract_version"],
    )
    _require_binding(calendar, rules)
    return rules


def _digest_file(path: Path, destination: Path | None = None) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream, (destination.open("wb") if destination else nullcontext()) as target:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
            if target is not None:
                target.write(chunk)
    return digest.hexdigest()


def _file_digests(directory: Path) -> dict[str, str]:
    return {name: _digest_file(directory / name) for name in _SCHEMAS
            if name != DAILY_REFERENCE_FILE or (directory / name).exists()}


def load_frozen_minute_scope(root, reference, *, manifest_digest: str, strict: bool = False) -> MinuteScopeInput:
    reference = MappingProxyType(validate_minute_scope_reference(reference))
    root = Path(root)
    manifest = _manifest(root, reference, expected_digest=manifest_digest)
    if "files" not in manifest or manifest["files"] != _file_digests(_directory(root, reference)):
        raise MinuteRuleSnapshotError("冻结分钟输入 Parquet 文件摘要不一致")
    scope = MinuteScopeInput(reference, root, manifest, manifest["files"], _frozen=True)
    if strict:
        if "rule_identity" in manifest:
            _ = scope.calendar
        else:
            _ = scope.rules
    return scope


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


def _coverage_payload(instruments, start_at, end_at, *, stock_raw=False) -> dict:
    from .time import require_aware_datetime

    if (not isinstance(instruments, (tuple, list)) or not instruments
            or any(not isinstance(code, str) or not code for code in instruments)
            or len(set(instruments)) != len(instruments)):
        raise MinuteRuleSnapshotError("规则覆盖须提供非空且不重复的证券")
    require_aware_datetime(start_at, "start_at")
    require_aware_datetime(end_at, "end_at")
    if start_at >= end_at:
        raise MinuteRuleSnapshotError("规则覆盖须提供有效时间范围")
    return {"instruments": sorted(instruments), "start_at": start_at.astimezone(timezone.utc).isoformat(),
            "end_at": end_at.astimezone(timezone.utc).isoformat(),
            **({"stock_raw": True} if stock_raw is True else {})}


def _coverage_digest(reference, manifest) -> str:
    return typed_canonical_hash({"scope": {name: reference[name] for name in _REFERENCE_KEYS},
        "rules_layout": manifest["rules_layout"], "coverage": manifest["admitted_rule_coverage"],
        "calendar": manifest["calendar"], "rules": manifest["rules"],
        "files": manifest["files"]})


def _write_scope_calendar(directory, calendar, rule_instruments):
    instruments = list(rule_instruments)
    instrument_indices = {item: index for index, item in enumerate(instruments)}
    templates = []
    template_indices = {}

    trading_dates = {}

    def policy_rows():
        for policy in calendar.policies:
            if policy.instrument not in instrument_indices:
                instrument_indices[policy.instrument] = len(instruments)
                instruments.append(policy.instrument)
            if policy.segments not in template_indices:
                template_indices[policy.segments] = len(templates)
                templates.append(policy.segments)
            # 单条政策转字典，不构造整包副本。
            key = id(policy.trading_dates)
            if key not in trading_dates:
                trading_dates[key] = [day.isoformat() for day in policy.trading_dates]
            row = policy._identity_payload(trading_dates=trading_dates[key])
            del row["instrument"], row["segments"]
            row.update(instrument_index=instrument_indices[policy.instrument],
                       segment_index=template_indices[policy.segments])
            row.setdefault("available_at", None)
            yield row

    _write_rows(directory, "policies.parquet", policy_rows())
    _write_rows(directory, "instruments.parquet", (
        {"instrument": item.to_dict(), "in_rules": index < len(rule_instruments)}
        for index, item in enumerate(instruments)
    ))
    _write_rows(directory, "segments.parquet", (
        {"segments": [item.to_dict() for item in template]} for template in templates
    ))


def _columnar_calendar_hash(manifest):
    return typed_canonical_hash({
        "contract_version": "minute-scope-columnar-calendar-identity-v1",
        "calendar_hash": manifest["reference"]["calendar_hash"],
        "calendar": manifest["calendar"],
        "files": {name: manifest["files"][name] for name in _CALENDAR_FILES},
    })


def _columnar_rules_hash(manifest):
    return typed_canonical_hash({
        "contract_version": manifest["rule_identity"],
        "calendar_hash": manifest["reference"]["calendar_hash"],
        "rules": manifest["rules"],
        "files": {name: manifest["files"][name] for name in (
            "instruments.parquet", "sources.parquet", "parameters.parquet",
            "rules.parquet", DAILY_REFERENCE_FILE,
        )},
    })


def _set_coverage(reference, manifest, coverage):
    if coverage:
        manifest["admitted_rule_coverage"] = coverage
        reference[_COVERAGE_DIGEST] = _coverage_digest(reference, manifest)
    else:
        manifest.pop("admitted_rule_coverage", None)
        reference.pop(_COVERAGE_DIGEST, None)
    manifest["reference"] = dict(reference)


class _DigestWriter:
    def __init__(self, stream):
        self.stream = stream
        self.digest = hashlib.sha256()

    @property
    def closed(self):
        return self.stream.closed

    def tell(self):
        return self.stream.tell()

    def write(self, data):
        size = self.stream.write(data)
        self.digest.update(data)
        return size


def write_columnar_minute_scope_input(root, *, name, calendar, bundle_id, sources, instruments,
                                     daily_rows, rules, parameter_sets, coverage_requests=()):
    """流式发布现货逐日参考批次及低频规则，并冻结实际读取文件的身份。"""
    from types import SimpleNamespace
    from .minute_scope_columnar import group_low_frequency_rules, normalize_daily_batch, validate_columnar_rules
    from .minute_rule_snapshots import load_minute_rule_snapshot_bundle

    binding = load_minute_rule_snapshot_bundle().capability_binding
    rule_metadata = SimpleNamespace(bundle_id=bundle_id, capability_binding=binding,
                                    contract_version=PROJECT_RULE_BUNDLE_VERSION)
    coverage_requests = tuple(coverage_requests)
    coverage = [_coverage_payload(**request) for request in coverage_requests]
    instruments = tuple(instruments)
    reference = validate_minute_scope_reference({
        "contract_version": MINUTE_SCOPE_INPUT_VERSION, "name": name,
        "calendar_hash": calendar.bundle_hash, "rules_hash": "0" * 64,
    })
    directory = _directory(Path(root), reference)
    if directory.exists():
        raise MinuteRuleSnapshotError("新分钟规则输入目录已存在")
    directory.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=directory.parent) as staging:
        staged = _directory(Path(staging), reference)
        staged.mkdir(parents=True)
        _write_scope_calendar(staged, calendar, instruments)
        source_input = sources if hasattr(sources, "__len__") else tuple(sources)
        manifest = _scope_manifest(reference, calendar, rule_metadata) | {
            "rules_layout": _DAILY_REFERENCE_LAYOUT, "rule_identity": _COLUMNAR_RULE_IDENTITY}
        scope = MinuteScopeInput(reference, Path(staging), manifest, {})
        object.__setattr__(scope, "_calendar_parts", (calendar, instruments))
        # 有些来源随行情批次补充低频规则及参数，按证券拿齐后在内存中验证。
        live = isinstance(rules, (list, tuple)) and isinstance(parameter_sets, (list, tuple))
        indexed = {}
        consumed = {}
        rule_offset = 0
        checked_parameters = []
        exhausted = False
        with (staged / DAILY_REFERENCE_FILE).open("wb") as output:
            sink = _DigestWriter(output)
            with pq.ParquetWriter(sink, DAILY_REFERENCE_SCHEMA, compression="zstd") as writer:
                def write_batches():
                    nonlocal rule_offset, exhausted
                    for batch in daily_rows:
                        normalized = normalize_daily_batch(batch)
                        if live:
                            for rule in rules[rule_offset:]:
                                indexed.setdefault(rule.instrument_id, []).append(rule)
                            rule_offset = len(rules)
                            checked_parameters.extend(dict(item) for item in parameter_sets[len(checked_parameters):])
                        writer.write_table(normalized, row_group_size=65536)
                        yield from normalized.to_batches(max_chunksize=32768)
                    exhausted = True

                def rules_for_code(code):
                    # 返回副本，逐日修订的展开不能改动低频索引。
                    values = sorted(indexed.get(code, ()), key=lambda rule: (rule.rule_id, rule.revision))
                    consumed[code] = tuple(values)
                    return values

                batches = write_batches()
                if live:
                    try:
                        validate_columnar_rules(scope, coverage_requests, sources=tuple(sorted(source_input, key=lambda item: item.source_id)),
                                                live_sources=source_input, daily_batches=batches,
                                                templates=checked_parameters, live_rules=rules_for_code)
                    except MinuteRuleSnapshotError:
                        if batches.gi_frame is None and not exhausted:
                            raise
                        # 允许输入流在结束时才补齐规则，最终按整套数据报告错误。
                        live = False
                for _ in batches:
                    pass
        sources = tuple(sorted(source_input, key=lambda item: item.source_id))
        daily_hash = sink.digest.hexdigest()
        rules = tuple(rules)
        _write_rows(staged, "sources.parquet", (item.to_dict() for item in sources))
        encoded = [json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
                   for item in parameter_sets]
        indices = {item: index for index, item in enumerate(encoded)}

        def low_frequency_rows():
            for rule in rules:
                row = rule.to_dict()
                parameters = json.dumps(row.pop("parameters"), ensure_ascii=False,
                                        sort_keys=True, separators=(",", ":"), allow_nan=False)
                if parameters not in indices:
                    indices[parameters] = len(encoded)
                    encoded.append(parameters)
                yield row | {"parameter_index": indices[parameters]}

        _write_rows(staged, "rules.parquet", low_frequency_rows())
        _write_rows(staged, "parameters.parquet", ({"parameters": item} for item in encoded))
        if live:
            grouped = group_low_frequency_rules(
                rules, {item.instrument_id: item for item in instruments}, {item.source_id: item for item in sources},
            )
            live = list(parameter_sets) == checked_parameters and all(
                consumed[code] == tuple(grouped.get(code, ())) for code in consumed
            )
        if not live:
            validate_columnar_rules(scope, coverage_requests, sources=sources, rules=rules)
        manifest["files"] = {name: daily_hash if name == DAILY_REFERENCE_FILE else _digest_file(staged / name)
                             for name in _SCHEMAS}
        reference[_CALENDAR_FILES_HASH] = _columnar_calendar_hash(manifest)
        reference["rules_hash"] = _columnar_rules_hash(manifest)
        _set_coverage(reference, manifest, coverage)
        (staged / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        from research_pipeline.platform.immutable_files import freeze_file
        for name in manifest["files"]:
            freeze_file(staged / name)
        staged.rename(directory)
    return reference


def write_minute_scope_input(root, *, name, calendar: SessionPolicyBundle,
                             rules: MinuteRuleSnapshotBundle, coverage_requests=()) -> dict:
    """写入完整版本流，已有同名输入只接受相同来源。"""
    reference = validate_minute_scope_reference({
        "contract_version": MINUTE_SCOPE_INPUT_VERSION, "name": name,
        "calendar_hash": calendar.bundle_hash, "rules_hash": rules.bundle_hash,
    })
    _require_binding(calendar, rules)
    root = Path(root)
    directory = _directory(root, reference)
    if directory.exists():
        try:
            published = validate_minute_scope_reference(
                json.loads((directory / "manifest.json").read_bytes())["reference"]
            )
        except (KeyError, TypeError, ValueError, OSError) as exc:
            raise MinuteRuleSnapshotError("现有分钟输入 manifest 无效") from exc
        if any(published.get(key) != value for key, value in reference.items()):
            raise MinuteRuleSnapshotError("同名分钟输入来源不一致")
        reference = published
        load_minute_scope_input(root, reference)
        if coverage_requests:
            raise MinuteRuleSnapshotError("规则覆盖证明只能随新的分钟输入发布")
        return reference
    from .minute_request_scope import require_minute_request_coverage

    coverage = []
    for request in coverage_requests:
        coverage.append(_coverage_payload(**request))
        require_minute_request_coverage(rules, calendar, **request)
    directory.mkdir(parents=True)
    _write_scope_calendar(directory, calendar, rules.instruments)
    _write_rows(directory, "sources.parquet", (item.to_dict() for item in rules.sources))
    parameter_values = []
    parameter_indices = {}
    shared_parameters = {}

    def rule_rows(*, daily):
        for rule in rules.rules:
            section = _DAILY_RULE_IDS.get(rule.rule_id) if rule.effective_from == rule.effective_to else None
            if (section is not None) != daily:
                continue
            daily_values = {}
            identity = id(rule.parameters)
            parameter_index = None if daily else shared_parameters.get(identity)
            if parameter_index is None:
                # 只在本次写入中复用共享的不可变标量模板；嵌套参数按当前内容编码。
                residual = dict(rule.parameters)
                if daily:
                    for key, value in rule.parameters:
                        dtype = _DAILY_FIELDS[section].get(key)
                        if ((dtype == pa.int64() and type(value) is int and -(1 << 63) <= value < 1 << 63)
                                or (dtype == pa.bool_() and type(value) is bool)
                                or (dtype == pa.string() and type(value) is str)):
                            daily_values[key] = residual.pop(key)
                encoded = json.dumps(residual, ensure_ascii=False, sort_keys=True,
                                     separators=(",", ":"), allow_nan=False)
                if encoded not in parameter_indices:
                    parameter_indices[encoded] = len(parameter_values)
                    parameter_values.append(encoded)
                parameter_index = parameter_indices[encoded]
                if not daily and all(type(value) in (str, int, float, bool, type(None)) for _, value in rule.parameters):
                    if len(shared_parameters) >= 8192:
                        shared_parameters.clear()
                    shared_parameters[identity] = parameter_index
            yield {
                "contract_version": rule.contract_version, "rule_id": rule.rule_id,
                "revision": rule.revision, "instrument_id": rule.instrument_id,
                "asset_class": rule.asset_class, "status": rule.status,
                "effective_from": rule.effective_from.isoformat(), "effective_to": rule.effective_to.isoformat(),
                "available_at": None if rule.available_at is None else rule.available_at.isoformat(timespec="seconds"),
                "source_ids": list(rule.source_ids), "unsupported_reason": rule.unsupported_reason,
                "parameter_index": parameter_index, **daily_values,
            }

    def daily_rows():
        for code, instrument_rows in groupby(rule_rows(daily=True), key=lambda row: row["instrument_id"]):
            days = {}
            for rule in instrument_rows:
                sections = days.setdefault(rule["effective_from"], {})
                sections.setdefault(_DAILY_RULE_IDS[rule["rule_id"]], []).append(rule)
            for day, sections in sorted(days.items()):
                for position in range(max(map(len, sections.values()))):
                    row = {"instrument_id": code, "trading_date": day}
                    for section, versions in sections.items():
                        if position < len(versions):
                            version = versions[position]
                            row["asset_class"] = version["asset_class"]
                            row.update({f"{section}_{name}": value for name, value in version.items()
                                        if name not in _DAILY_SHARED})
                    yield row

    _write_rows(directory, "rules.parquet", rule_rows(daily=False))
    _write_rows(directory, DAILY_REFERENCE_FILE, daily_rows())
    _write_rows(directory, "parameters.parquet", ({"parameters": item} for item in parameter_values))
    manifest = _scope_manifest(reference, calendar, rules) | {"rules_layout": _DAILY_REFERENCE_LAYOUT}
    manifest["files"] = _file_digests(directory)
    reference[_CALENDAR_FILES_HASH] = _columnar_calendar_hash(manifest)
    _set_coverage(reference, manifest, coverage)
    (directory / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return reference


def _scope_manifest(reference, calendar, rules) -> dict:
    return {
        "reference": dict(reference),
        "calendar": {"bundle_id": calendar.bundle_id, "capability_binding": dict(calendar.capability_binding),
                     "inventory_evidence_hash": calendar.inventory_evidence_hash,
                     "contract_version": calendar.contract_version},
        "rules": {"bundle_id": rules.bundle_id, "capability_binding": dict(rules.capability_binding),
                  "contract_version": rules.contract_version},
    }


def _copy_scope_files(source: MinuteScopeInput, root: Path, *, frozen: bool) -> tuple[dict, str | None]:
    reference = validate_minute_scope_reference(source.reference)
    if source._metadata["reference"] != reference:
        raise MinuteRuleSnapshotError("分钟输入对象与 descriptor 来源不一致")
    if _manifest(Path(source.root), reference) != source._metadata:
        raise MinuteRuleSnapshotError("已加载分钟输入清单发生变化")
    target = _directory(root, reference)
    if target.exists() and not frozen:
        load_minute_scope_input(root, reference)
        return {}, None
    target.mkdir(parents=True)
    try:
        directory = _directory(source.root, reference)
        for filename, expected in source.files.items():
            source_file, target_file = directory / filename, target / filename
            linked = frozen and link_readonly_file(source_file, target_file)
            digest = _digest_file(target_file) if linked else _digest_file(source_file, target_file)
            if digest != expected:
                raise MinuteRuleSnapshotError(f"已准入分钟输入文件发生变化：{filename}")
            if frozen and not linked:
                freeze_file(target_file)
        if frozen:
            manifest = dict(source._metadata)
            manifest["files"] = dict(source.files)
            content = json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8")
        else:
            manifest = {}
            content = (directory / "manifest.json").read_bytes()
        (target / "manifest.json").write_bytes(content)
    except Exception:
        remove_tree(target)
        raise
    return manifest, hashlib.sha256(content).hexdigest() if frozen else None


def copy_minute_scope_input(source: MinuteScopeInput, root) -> dict:
    """按准入证据复制完整的包来源。"""
    _copy_scope_files(source, Path(root), frozen=False)
    return dict(source.reference)


def freeze_minute_scope_input(source: MinuteScopeInput, root: Path, *, final_root: Path) -> tuple[MinuteScopeInput, str]:
    """复制并核对准入文件，向同次运行交接已验证的冻结引用。"""
    manifest, digest = _copy_scope_files(source, root, frozen=True)
    return MinuteScopeInput(source.reference, final_root, manifest, manifest["files"],
                            _admitted_calendar=vars(source).get("_calendar_parts"), _frozen=True), digest
