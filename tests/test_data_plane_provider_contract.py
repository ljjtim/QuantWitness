from __future__ import annotations

from datetime import date

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from research_pipeline.catalog import (
    CompiledCatalog,
    DriftAttestation,
    PolicyContract,
)
from research_pipeline.catalog.discovery import ObjectExecutionEvidence
from research_pipeline.data_plane import (
    DataPlaneExecutionBudget,
    DuckDBColumnarProvider,
    FilterOperator,
    FilterPredicate,
    ParquetColumnarProvider,
    ProviderExecutionError,
    QueryBudget,
    QueryIR,
    QueryPurpose,
    SortKey,
    DateRangeV1,
    UniverseSelection,
    admit_query,
)
from research_pipeline.data_plane.execution_estimate import build_execution_estimate
from research_pipeline.data_plane.gates import _expected_arrow_type
from research_pipeline.data_plane.providers.sql import _duckdb_type
from research_pipeline.platform import typed_canonical_hash


def _plan(*, max_rows=100, max_bytes=1_000_000, batch_size=2, end=date(2025, 1, 10), instruments=("000001.XSHE", "600000.XSHG")):
    catalog = CompiledCatalog.load_default()
    binding = catalog.bindings["cn_equity.daily_bar.local.v1"]
    policy = PolicyContract(**dict(catalog.policies[binding["drift_policy_id"]]))
    attestation = DriftAttestation(
        catalog.catalog_hash,
        binding["binding_id"],
        binding["source_profile"],
        binding["environment"],
        binding["expected_schema_revision"],
        binding["expected_schema_revision"],
        policy.content_hash,
        True,
    )
    query = QueryIR(
        "cn_equity.daily_bar",
        int(catalog.datasets["cn_equity.daily_bar"]["dataset_version"]),
        ("fld_equity_daily_date", "fld_equity_daily_code", "fld_equity_daily_close"),
        QueryPurpose.FEATURE,
        DateRangeV1(date(2025, 1, 2), end),
        UniverseSelection(tuple(instruments)),
        (FilterPredicate("fld_equity_daily_close", FilterOperator.RANGE, (0.0, 1000.0)),),
        (SortKey("fld_equity_daily_date"), SortKey("fld_equity_daily_code")),
        QueryBudget(max_rows, max_bytes, batch_size),
        adjustment="unadjusted",
    )
    return admit_query(query, catalog=catalog, binding=binding, attestation=attestation)


def _rows():
    return {
        "date": [date(2025, 1, 3), date(2025, 1, 2), date(2025, 1, 2)],
        "code": ["000001.XSHE", "600000.XSHG", "000001.XSHE"],
        "close": [11.0, 12.0, 10.0],
    }


def _execution_budget():
    return DataPlaneExecutionBudget(
        memory_bytes=256 * 1024 * 1024,
        temp_bytes=0,
        cpu_slots=1,
    )


def _execution_estimate(plan, budget=None):
    actual_budget = budget or _execution_budget()
    variable_fields = tuple(
        field_id
        for field_id in plan.temporal_selection.required_scan_fields
        if dict(plan.field_types)[field_id].lower() == "string"
    )
    return build_execution_estimate(
        plan,
        evidence=ObjectExecutionEvidence(
            object_name=plan.object_name,
            object_kind="physical_table",
            dependency_chain=(plan.object_name,),
            source_rows_upper=min(plan.query.budget.max_rows, 1_000),
            expanded_rows_upper=None,
            variable_width_upper=tuple(
                (field_id, 128) for field_id in sorted(variable_fields)
            ),
            has_json_expansion=False,
            has_window=False,
            has_order_by=True,
            query_scope_hash=typed_canonical_hash(plan.query.to_dict()),
            database_revision="a" * 64,
            expansion_bound_method=None,
        ),
        execution_budget=actual_budget,
    )


def _data_plane_execution_budget(operator_plan, registry):
    from research_pipeline.runtime.operator_registry import compile_operator_graph_dag

    materialize_ids = {
        node.node_id
        for node in operator_plan.recipe.nodes
        if node.operator_id == "data.columnar.materialize"
    }
    matches = tuple(
        node
        for node in compile_operator_graph_dag(operator_plan, registry).nodes
        if node.node_id in materialize_ids
    )
    assert len(matches) == 1
    return DataPlaneExecutionBudget.from_resource_budget(matches[0].resource_budget)


def test_duckdb_and_parquet_share_arrow_contract(tmp_path):
    plan = _plan()
    db = tmp_path / "source.duckdb"
    with duckdb.connect(str(db)) as connection:
        connection.execute("CREATE TABLE daily_price_unadjusted(date DATE, code VARCHAR, close DOUBLE)")
        connection.execute(
            "INSERT INTO daily_price_unadjusted VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?)",
            [date(2025, 1, 3), "000001.XSHE", 11.0, date(2025, 1, 2), "600000.XSHG", 12.0, date(2025, 1, 2), "000001.XSHE", 10.0],
        )
    parquet = tmp_path / "source.parquet"
    pq.write_table(pa.table(_rows()), parquet)

    outputs = []
    for provider in (
        DuckDBColumnarProvider(db),
        ParquetColumnarProvider(parquet, allowed_root=tmp_path),
    ):
        budget = _execution_budget()
        with provider.open_stream(
            plan,
            execution_budget=budget,
            execution_estimate=_execution_estimate(plan, budget),
            scratch_root=tmp_path / "scratch",
        ) as stream:
            table = stream.read_table()
            outputs.append(table.to_pylist())
            assert stream.closed
            assert table.column_names == list(plan.query.field_ids)
    assert outputs[0] == outputs[1]
    assert outputs[0][0]["fld_equity_daily_code"] == "000001.XSHE"


def test_stream_budget_closes_resources(tmp_path):
    plan = _plan(max_rows=1, batch_size=2)
    db = tmp_path / "source.duckdb"
    with duckdb.connect(str(db)) as connection:
        connection.execute("CREATE TABLE daily_price_unadjusted AS SELECT * FROM (VALUES (DATE '2025-01-02','000001.XSHE',10.0),(DATE '2025-01-03','000001.XSHE',11.0)) t(date,code,close)")
    budget = _execution_budget()
    stream = DuckDBColumnarProvider(db).open_stream(
        plan,
        execution_budget=budget,
        execution_estimate=_execution_estimate(plan, budget),
        scratch_root=tmp_path / "scratch",
    )
    with pytest.raises(ProviderExecutionError, match="超过预算"):
        next(stream)
    assert stream.closed


def test_parquet_rejects_path_escape_and_uncommitted_directory(tmp_path):
    outside = tmp_path.parent / "outside.parquet"
    pq.write_table(pa.table(_rows()), outside)
    with pytest.raises(ProviderExecutionError, match="允许根目录"):
        ParquetColumnarProvider(outside, allowed_root=tmp_path)
    uncommitted = tmp_path / "snapshot"
    uncommitted.mkdir()
    pq.write_table(pa.table(_rows()), uncommitted / "part-00000.parquet")
    with pytest.raises(ProviderExecutionError, match="尚未提交"):
        ParquetColumnarProvider(uncommitted, allowed_root=tmp_path)
    outside.unlink()


@pytest.mark.parametrize(
    ("logical_type", "duckdb_type", "arrow_type"),
    [
        ("decimal128", "DECIMAL(38,18)", "decimal128(38, 18)"),
        ("int8", "TINYINT", "int8"),
        ("int16", "SMALLINT", "int16"),
        ("timestamp", "TIMESTAMP", "timestamp[us]"),
    ],
)
def test_provider_and_quality_gate_share_extended_logical_types(
    logical_type: str,
    duckdb_type: str,
    arrow_type: str,
) -> None:
    assert _duckdb_type(logical_type) == duckdb_type
    assert _expected_arrow_type(logical_type) == arrow_type
