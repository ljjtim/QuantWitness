"""正式因子库发布身份的只读观察与复验。"""

from __future__ import annotations

from contextlib import nullcontext
from datetime import date
import hashlib
import json
from pathlib import Path

from factor_contracts import (
    FactorEvidenceBinding,
    FactorImplementationBinding,
    FactorPublicationBinding,
    FactorReadLease,
    FactorStorageBinding,
    factor_catalog_hash,
    canonical_json,
)

from .errors import SnapshotIntegrityError


FACTOR_SCHEMA_NAME = "factor_database"
FACTOR_AVAILABILITY_POLICY_REF = "available.factor_publication.v1"
FACTOR_REVISION_POLICY_REF = "revision.factor_publication.v1"


def _read_compute_code_hash(
    staging_path: object,
    *,
    expected_manifest_hash: str,
    source_database_identity: str,
    catalog_hash: str,
    code_commit: str,
) -> str:
    audit_root = Path(str(staging_path)).resolve()
    if not audit_root.is_dir():
        raise SnapshotIntegrityError("因子计算审计目录不存在")

    def read_json(name: str) -> object:
        path = audit_root / name
        if not path.is_file():
            raise SnapshotIntegrityError(f"因子计算审计文件缺失: {name}")
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise SnapshotIntegrityError(f"因子计算审计文件无法读取: {name}") from exc

    expected_manifest = read_json("expected_manifest.json")
    work_manifest = read_json("work_manifest.json")
    next_manifest = read_json("next_manifest.json")
    audit_report = read_json("audit_report.json")
    compute_evidence = read_json("compute_evidence.json")
    storage_manifest_path = audit_root / "storage_manifest.json"
    storage_manifest = (
        read_json("storage_manifest.json")
        if storage_manifest_path.is_file()
        else None
    )
    if (
        not isinstance(work_manifest, dict)
        or set(work_manifest) != {"tables"}
        or not isinstance(next_manifest, dict)
        or set(next_manifest) != {"tables"}
        or not isinstance(compute_evidence, dict)
    ):
        raise SnapshotIntegrityError("因子计算审计文件schema不匹配")
    missing_evidence = audit_report.get("custom_missing_evidence")
    if missing_evidence is not None:
        if (
            not isinstance(missing_evidence, dict)
            or set(missing_evidence) != {"path", "sha256", "row_count", "reasons"}
        ):
            raise SnapshotIntegrityError("自定义因子缺失审计schema不匹配")
        evidence_path = audit_root / str(missing_evidence["path"])
        if not evidence_path.is_file():
            raise SnapshotIntegrityError("自定义因子缺失审计文件不存在")
        digest = hashlib.sha256()
        with evidence_path.open("rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
        if digest.hexdigest() != missing_evidence["sha256"]:
            raise SnapshotIntegrityError("自定义因子缺失审计文件身份不一致")
    audit_identity = {
        "expected_manifest": expected_manifest,
        "work_manifest": work_manifest["tables"],
        "next_manifest": next_manifest["tables"],
        "audit_report": audit_report,
        "compute_evidence": compute_evidence,
    }
    storage_tables = (
        None if storage_manifest is None else storage_manifest.get("tables")
    )
    actual_manifest_hashes = {
        hashlib.sha256(canonical_json(audit_identity).encode("utf-8")).hexdigest(),
        hashlib.sha256(
            canonical_json({**audit_identity, "storage_manifest": storage_tables}).encode(
                "utf-8"
            )
        ).hexdigest(),
    }
    if expected_manifest_hash not in actual_manifest_hashes:
        raise SnapshotIntegrityError("因子计算审计链与数据库manifest_hash不一致")
    identity = compute_evidence.get("identity")
    if not isinstance(identity, dict):
        raise SnapshotIntegrityError("因子计算审计缺少实现身份")
    if (
        identity.get("source_identity_hash") != source_database_identity
        or identity.get("catalog_hash") != catalog_hash
        or identity.get("code_commit") != code_commit
    ):
        raise SnapshotIntegrityError("因子计算审计身份与数据库记录不一致")
    compute_code_hash = identity.get("compute_code_hash")
    if (
        not isinstance(compute_code_hash, str)
        or len(compute_code_hash) != 64
        or any(character not in "0123456789abcdef" for character in compute_code_hash)
    ):
        raise SnapshotIntegrityError("因子计算审计缺少有效compute_code_hash")
    return compute_code_hash


def _require_catalog_relations(
    connection,
    *,
    snapshot: dict[str, object],
    publication_id: str,
    publication_date_start: str,
    publication_date_end: str,
    verified_relations: tuple[str, ...],
) -> FactorEvidenceBinding:
    recipes = snapshot["recipes"]
    sources = snapshot["sources"]
    if not isinstance(recipes, list) or not isinstance(sources, list):
        raise SnapshotIntegrityError("发布Catalog正文结构无效")
    expected_definitions = sorted(
        (
            str(item["factor_id"]),
            bool(item["enabled"]),
            str(item["formula_hash"]),
            str(item["recipe_hash"]),
            str(item["source_reference_id"]),
            item["storage_table"],
            item["storage_column"],
            str(item["quality_grade"]),
            str(item["availability_policy"]),
        )
        for item in recipes
    )
    actual_definitions = connection.execute(
        """
        SELECT factor_id, enabled, formula_hash, recipe_hash, source_reference_id,
               storage_table, storage_column, quality_grade, availability_policy
        FROM factor_definition ORDER BY factor_id
        """
    ).fetchall()
    if actual_definitions != expected_definitions:
        raise SnapshotIntegrityError("factor_definition与发布Catalog正文不一致")

    expected_dependencies = sorted(
        (
            str(recipe["factor_id"]),
            str(item["dependency_type"]),
            str(item["dependency_id"]),
            str(item["dependency_role"]),
            int(item["lookback"]),
            str(item["availability_policy"]),
        )
        for recipe in recipes
        for item in recipe["dependencies"]
    )
    actual_dependencies = connection.execute(
        """
        SELECT factor_id, dependency_type, dependency_id, dependency_role,
               lookback, availability_policy
        FROM factor_dependency
        ORDER BY factor_id, dependency_type, dependency_id, dependency_role,
                 lookback, availability_policy
        """
    ).fetchall()
    if actual_dependencies != expected_dependencies:
        raise SnapshotIntegrityError("factor_dependency与发布Catalog正文不一致")

    expected_steps = sorted(
        (
            (
                str(recipe["factor_id"]),
                str(item["step_id"]),
                index,
                str(item["operator"]),
                item["inputs"],
                item["parameters"],
                str(item["output"]),
                str(item["availability_policy"]),
            )
            for recipe in recipes
            for index, item in enumerate(recipe["steps"])
        ),
        key=lambda item: (item[0], item[2], item[1]),
    )
    actual_steps = [
        (
            str(row[0]),
            str(row[1]),
            int(row[2]),
            str(row[3]),
            json.loads(row[4]),
            json.loads(row[5]),
            str(row[6]),
            str(row[7]),
        )
        for row in connection.execute(
            """
            SELECT factor_id, step_id, step_order, operator, inputs_json,
                   parameters_json, output, availability_policy
            FROM factor_recipe_step
            ORDER BY factor_id, step_order, step_id
            """
        ).fetchall()
    ]
    if actual_steps != expected_steps:
        raise SnapshotIntegrityError("factor_recipe_step与发布Catalog正文不一致")

    source_fields = (
        "source_reference_id",
        "source_name",
        "document_title",
        "author_or_vendor",
        "publication_year",
        "section",
        "url",
        "local_document_path",
        "notes",
    )
    expected_sources = sorted(tuple(item[field] for field in source_fields) for item in sources)
    actual_sources = connection.execute(
        """
        SELECT source_reference_id, source_name, document_title,
               author_or_vendor, publication_year, section, url,
               local_document_path, notes
        FROM factor_source_reference ORDER BY source_reference_id
        """
    ).fetchall()
    if actual_sources != expected_sources:
        raise SnapshotIntegrityError("factor_source_reference与发布Catalog正文不一致")

    enabled_ids = {
        str(item["factor_id"]) for item in recipes if item["enabled"] is True
    }
    quality_rows = connection.execute(
        """
        SELECT factor_id, quality_status
        FROM factor_quality_summary WHERE publication_id=?
        ORDER BY factor_id
        """,
        [publication_id],
    ).fetchall()
    if {str(row[0]) for row in quality_rows} != enabled_ids:
        raise SnapshotIntegrityError("factor_quality_summary未逐项覆盖启用配方")
    quality_status_counts: dict[str, int] = {}
    for _, status in quality_rows:
        quality_status_counts[str(status)] = quality_status_counts.get(str(status), 0) + 1

    anomaly_ids = {
        str(row[0])
        for row in connection.execute(
            "SELECT DISTINCT factor_id FROM factor_numeric_anomalies"
        ).fetchall()
    }
    recipe_ids = {str(item["factor_id"]) for item in recipes}
    if not anomaly_ids <= recipe_ids:
        raise SnapshotIntegrityError("factor_numeric_anomalies引用未知配方")
    anomaly_bounds = connection.execute(
        """
        SELECT count(*), CAST(min(trade_date) AS VARCHAR),
               CAST(max(trade_date) AS VARCHAR)
        FROM factor_numeric_anomalies
        """
    ).fetchone()
    anomaly_count = int(anomaly_bounds[0])
    if anomaly_count and (
        anomaly_bounds[1] < publication_date_start
        or anomaly_bounds[2] > publication_date_end
    ):
        raise SnapshotIntegrityError("factor_numeric_anomalies超出发布覆盖日期")
    anomaly_reason_counts = tuple(
        (str(reason), int(count))
        for reason, count in connection.execute(
            """
            SELECT reason, count(*) FROM factor_numeric_anomalies
            GROUP BY reason ORDER BY reason
            """
        ).fetchall()
    )
    return FactorEvidenceBinding(
        recipe_count=len(recipes),
        enabled_recipe_count=len(enabled_ids),
        dependency_count=len(expected_dependencies),
        recipe_step_count=len(expected_steps),
        source_reference_count=len(sources),
        quality_record_count=len(quality_rows),
        quality_status_counts=tuple(sorted(quality_status_counts.items())),
        numeric_anomaly_count=anomaly_count,
        numeric_anomaly_reason_counts=anomaly_reason_counts,
        verified_relations=verified_relations,
    )


def observe_factor_publication(
    database: str | Path,
    *,
    storage_table: str,
    acquire_lease: bool = True,
) -> FactorPublicationBinding:
    """按请求表读取当前published版本，不接受draft或混合版本。"""

    import duckdb

    path = Path(database).resolve()
    lease = FactorReadLease(path) if acquire_lease else nullcontext()
    with lease, duckdb.connect(str(path), read_only=True) as connection:
        schema_row = connection.execute(
            "SELECT schema_version FROM factor_schema_version WHERE schema_name=?",
            [FACTOR_SCHEMA_NAME],
        ).fetchone()
        if schema_row is None:
            raise SnapshotIntegrityError("因子库缺少正式Schema版本")
        storage = connection.execute(
            """
            SELECT storage_table, content_hash, CAST(date_start AS VARCHAR),
                   CAST(date_end AS VARCHAR), publication_id, catalog_hash
            FROM factor_storage_state WHERE storage_table=?
            """,
            [storage_table],
        ).fetchone()
        if storage is None or storage[4] is None or storage[5] is None:
            raise SnapshotIntegrityError(f"因子发布缺少存储状态: {storage_table}")
        publication = connection.execute(
            """
            SELECT publication_id, compute_run_id, CAST(published_at AS VARCHAR),
                   catalog_hash, CAST(date_start AS VARCHAR), CAST(date_end AS VARCHAR),
                   factor_count, row_count, status
            FROM factor_publication WHERE publication_id=?
            """,
            [storage[4]],
        ).fetchone()
        if publication is None or publication[8] != "published":
            raise SnapshotIntegrityError("当前因子存储状态未绑定published版本")
        if storage[5] != publication[3]:
            raise SnapshotIntegrityError(f"因子存储表混用了其他Catalog身份: {storage_table}")
        run = connection.execute(
            """
            SELECT source_database_identity, formula_catalog_hash, code_commit,
                   manifest_hash, status, staging_path
            FROM factor_compute_run WHERE compute_run_id=?
            """,
            [publication[1]],
        ).fetchone()
        if run is None or run[4] != "validated":
            raise SnapshotIntegrityError("因子发布未绑定validated计算运行")
        if run[1] != publication[3]:
            raise SnapshotIntegrityError("计算运行与发布Catalog身份不一致")
        compute_code_hash = _read_compute_code_hash(
            run[5],
            expected_manifest_hash=str(run[3]),
            source_database_identity=str(run[0]),
            catalog_hash=str(run[1]),
            code_commit=str(run[2]),
        )
        snapshot_rows = connection.execute(
            "SELECT snapshot_json FROM factor_catalog_snapshot WHERE catalog_hash=?",
            [publication[3]],
        ).fetchall()
        if len(snapshot_rows) != 1:
            raise SnapshotIntegrityError("发布Catalog正文缺失或重复")
        try:
            snapshot = json.loads(snapshot_rows[0][0])
            if (
                set(snapshot) != {"snapshot_version", "recipes", "sources"}
                or snapshot["snapshot_version"] != "factor-catalog-snapshot-v1"
                or factor_catalog_hash(snapshot["recipes"], snapshot["sources"])
                != publication[3]
            ):
                raise ValueError("正文身份不一致")
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise SnapshotIntegrityError("发布Catalog正文无法复验") from exc
        mixed_storage_count = connection.execute(
            """
            SELECT count(*) FROM factor_storage_state
            WHERE publication_id IS NULL OR catalog_hash IS NULL
               OR publication_id<>? OR catalog_hash<>?
            """,
            [publication[0], publication[3]],
        ).fetchone()[0]
        if mixed_storage_count:
            raise SnapshotIntegrityError("factor_storage_state混用了其他发布身份")
        evidence = _require_catalog_relations(
            connection,
            snapshot=snapshot,
            publication_id=str(publication[0]),
            publication_date_start=str(publication[4]),
            publication_date_end=str(publication[5]),
            verified_relations=(),
        )
        return FactorPublicationBinding(
            publication_id=str(publication[0]),
            compute_run_id=str(publication[1]),
            published_at=str(publication[2]),
            catalog_hash=str(publication[3]),
            date_start=str(publication[4]),
            date_end=str(publication[5]),
            factor_count=int(publication[6]),
            row_count=int(publication[7]),
            schema_version=str(schema_row[0]),
            availability_policy_ref=FACTOR_AVAILABILITY_POLICY_REF,
            revision_policy_ref=FACTOR_REVISION_POLICY_REF,
            implementation=FactorImplementationBinding(
                source_database_identity=str(run[0]),
                formula_catalog_hash=str(run[1]),
                code_commit=str(run[2]),
                compute_code_hash=compute_code_hash,
                manifest_hash=str(run[3]),
            ),
            storage=(
                FactorStorageBinding(
                    storage_table=str(storage[0]),
                    content_hash=str(storage[1]),
                    date_start=str(storage[2]),
                    date_end=str(storage[3]),
                ),
            ),
            evidence=evidence,
        )


def require_factor_publication_unchanged(
    expected: FactorPublicationBinding,
    database: str | Path,
    *,
    storage_table: str,
) -> None:
    actual = observe_factor_publication(
        database,
        storage_table=storage_table,
        acquire_lease=False,
    )
    if actual != expected:
        raise SnapshotIntegrityError("因子发布身份在准入或读取期间发生变化")


def require_factor_query_covered(
    binding: FactorPublicationBinding,
    *,
    storage_table: str,
    start: date,
    end: date,
) -> None:
    storage = binding.storage_map.get(storage_table)
    if storage is None:
        raise SnapshotIntegrityError(f"因子publication未绑定存储表: {storage_table}")
    if start < date.fromisoformat(storage.date_start) or end > date.fromisoformat(
        storage.date_end
    ):
        raise SnapshotIntegrityError("因子查询日期超出当前存储状态覆盖范围")


__all__ = [
    "FACTOR_AVAILABILITY_POLICY_REF",
    "FACTOR_REVISION_POLICY_REF",
    "observe_factor_publication",
    "require_factor_query_covered",
    "require_factor_publication_unchanged",
]
