from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
import json
from pathlib import Path
import shutil
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import pytest
import yaml

from research_pipeline.catalog import CompiledCatalog, DriftAttestation, PolicyContract
from research_pipeline.application.research_run import _runtime_data_bundle
from research_pipeline.application.research_plan_store import _formal_input_lineage
from research_pipeline.data_plane import (
    DatasetPartitionRef,
    PartitionedDatasetRef,
    admit_query,
)
from research_pipeline.packages import (
    ResearchPackageError,
    compile_research_package,
    load_research_package,
)
from research_pipeline.results import ResultAssembler, ResultStore, load_result_metrics
from research_pipeline.runtime import (
    AuditEnvironmentManifest,
    ExecutionMode,
    ExternalArtifactStore,
    RuntimeNodeContext,
    RuntimeNodeValue,
    RuntimeExecutionService,
    RuntimeWorkerError,
)
from research_pipeline.platform import canonical_json, typed_canonical_hash
from research_pipeline.runtime.operator_runtime import OperatorRuntimeContext
from research_pipeline.runtime.adapters.common import ResearchRunEnvironment
from research_pipeline.runtime.adapters.minute_adjustment import _actions_in_applicable_range, _etf_action_rows, _temporal_corporate_action_rows
from research_pipeline.runtime.adapters.minute_research import execute_research_features_intraday_v1, execute_research_labels_intraday_v1
from research_pipeline.runtime.adapters.minute_data import execute_research_observation_minute_bars_v1
from research_pipeline.data_plane import build_adjustment_factor_snapshot
from research_pipeline.domain import CorporateAction
from research_pipeline.runtime.operator_definitions import build_mainline_operator_manifest
from research_pipeline.runtime.operator_registry import (
    build_mainline_operator_registry,
    compile_operator_graph_dag,
)
from minute_observation_fixture import prepare_observation_package

ROOT = Path(__file__).resolve().parents[1]
REFERENCES = ROOT / "research_packages/references"


def test_action_effective_time_uses_datetime_half_open_range() -> None:
    zone = datetime.fromisoformat("2024-01-05T09:31:00+08:00").tzinfo
    action = CorporateAction(
        action_id="end-day",
        revision=1,
        kind="cash_dividend",
        instrument_hash="1" * 64,
        announcement_available_time=datetime(2024, 1, 2, 9, 31, tzinfo=zone),
        record_date=date(2024, 1, 4),
        ex_date=date(2024, 1, 5),
        pay_date=date(2024, 1, 8),
        effective_date=date(2024, 1, 5),
        cash_per_share_microunits=100_000,
    )

    included, times = _actions_in_applicable_range(
        (action,),
        applicable_start=datetime(2024, 1, 2, 9, 31, tzinfo=zone),
        applicable_end=datetime(2024, 1, 5, 15, 1, tzinfo=zone),
    )
    excluded, _ = _actions_in_applicable_range(
        (action,),
        applicable_start=datetime(2024, 1, 2, 9, 31, tzinfo=zone),
        applicable_end=datetime(2024, 1, 5, 9, 31, tzinfo=zone),
    )

    assert included == (action,)
    assert times[action.effective_date] == datetime(
        2024, 1, 5, 9, 31, tzinfo=zone
    )
    assert excluded == ()


def test_etf_action_source_code_resolves_to_canonical_instrument_in_action_window() -> None:
    rows = _etf_action_rows(
        ({
            "fld_etf_ca_id": 105192,
            "fld_etf_ca_code": "510300",
            "fld_etf_ca_pub_date": date(2024, 1, 11),
            "fld_etf_ca_event_id": 404001,
            "fld_etf_ca_event": "现金分红",
            "fld_etf_ca_process_id": 405002,
            "fld_etf_ca_cash_per_share": "0.069",
            "fld_etf_ca_split_ratio": None,
            "fld_etf_ca_record_date": date(2024, 1, 17),
            "fld_etf_ca_ex_date": date(2024, 1, 18),
            "fld_etf_ca_pay_date": date(2024, 1, 23),
            "fld_etf_ca_status": 0,
            "fld_etf_ca_add_time": datetime(2024, 1, 11, 10, 19, 42),
            "fld_etf_ca_mod_time": datetime(2024, 1, 11, 10, 19, 42),
        },),
        instrument_id="510300.XSHG",
    )

    assert len(rows) == 1
    assert rows[0]["code"] == "510300"


def _stock_action_history_fixture(*, revised_at: datetime):
    required = (
        "fld_ca_id", "fld_ca_code", "fld_ca_implementation_pub_date",
        "fld_ca_add_time", "fld_ca_mod_time", "fld_ca_ex_date", "fld_ca_note",
    )
    row = {
        "fld_ca_id": 1,
        "fld_ca_code": "000001.XSHE",
        "fld_ca_implementation_pub_date": date(2024, 1, 2),
        "fld_ca_cash_per_ten": "1.0",
        "fld_ca_stock_per_ten": "0",
        "fld_ca_transfer_per_ten": "0",
        "fld_ca_record_date": date(2024, 1, 3),
        "fld_ca_ex_date": date(2024, 1, 4),
        "fld_ca_cash_arrival_date": date(2024, 1, 5),
        "fld_ca_stock_arrival_date": None,
        "fld_ca_listing_date": None,
        "fld_ca_plan_progress": "实施方案",
        "fld_ca_status": 0,
        "fld_ca_add_time": datetime(2024, 1, 2, 8),
        "fld_ca_mod_time": revised_at,
        "fld_ca_note": None,
    }
    plan = SimpleNamespace(
        query=SimpleNamespace(field_ids=tuple(row)),
        temporal_selection=SimpleNamespace(
            requires_consumer_binding=True,
            visibility_filter=SimpleNamespace(
                available_time_field="fld_ca_implementation_pub_date",
                inclusive=False,
                additional_time_fields=(),
            ),
            revision_selector=SimpleNamespace(order_fields=("fld_ca_implementation_pub_date",)),
        ),
    )

    class FakeDataset:
        def iter_temporal_fact_batches(self, **_kwargs):
            yield pa.RecordBatch.from_pylist([{field: row[field] for field in required}])

    return plan, FakeDataset(), row


def test_stock_action_uses_announcement_despite_later_maintenance(monkeypatch) -> None:
    from research_pipeline.runtime.adapters import minute_adjustment as operator_adapters

    plan, dataset, row = _stock_action_history_fixture(
        revised_at=datetime(2025, 1, 5, 8)
    )
    monkeypatch.setattr(operator_adapters, "_columnar_request_dataset",
                        lambda *_args: (dataset, SimpleNamespace(), plan))
    times = []
    def rows_at(*_args, consumer_time, **_kwargs):
        times.append(consumer_time)
        return (dict(row),), SimpleNamespace(), plan
    monkeypatch.setattr(operator_adapters, "_columnar_request_rows_at", rows_at)
    rows, _reference = _temporal_corporate_action_rows(
        object(), "stock_corporate_actions", instrument_id="000001.XSHE",
        asset_class="cn_stock",
        trading_sessions=tuple(date(2024, 1, day) for day in (2, 3, 4, 5)),
        applicable_start=datetime.fromisoformat("2024-01-02T09:31:00+08:00"),
        applicable_end=datetime.fromisoformat("2024-01-05T15:01:00+08:00"),
        as_of=datetime.fromisoformat("2024-01-05T15:01:00+08:00"),
    )
    assert len(rows) == 1
    assert times == [datetime.fromisoformat("2024-01-03T09:15:00+08:00")]


def test_etf_action_missing_window_revision_fails_closed(monkeypatch) -> None:
    from research_pipeline.runtime.adapters import minute_adjustment as operator_adapters

    row = {
        "fld_etf_ca_id": 105192,
        "fld_etf_ca_code": "510300",
        "fld_etf_ca_pub_date": date(2024, 1, 2),
        "fld_etf_ca_event_id": 404001,
        "fld_etf_ca_event": "现金分红",
        "fld_etf_ca_process_id": 405002,
        "fld_etf_ca_cash_per_share": "0.069",
        "fld_etf_ca_split_ratio": None,
        "fld_etf_ca_record_date": date(2024, 1, 3),
        "fld_etf_ca_ex_date": date(2024, 1, 4),
        "fld_etf_ca_pay_date": date(2024, 1, 5),
        "fld_etf_ca_status": 0,
        "fld_etf_ca_add_time": datetime(2024, 1, 2, 8),
        "fld_etf_ca_mod_time": datetime(2024, 1, 4, 10),
    }
    required = (
        "fld_etf_ca_id", "fld_etf_ca_code", "fld_etf_ca_pub_date",
        "fld_etf_ca_add_time", "fld_etf_ca_mod_time", "fld_etf_ca_ex_date",
    )
    plan = SimpleNamespace(
        query=SimpleNamespace(field_ids=tuple(row)),
        temporal_selection=SimpleNamespace(
            requires_consumer_binding=True,
            visibility_filter=SimpleNamespace(
                available_time_field="fld_etf_ca_pub_date",
                inclusive=False,
                additional_time_fields=("fld_etf_ca_add_time",),
            ),
            revision_selector=SimpleNamespace(
                order_fields=("fld_etf_ca_mod_time",)
            ),
        ),
    )

    class FakeDataset:
        def iter_temporal_fact_batches(self, **_kwargs):
            yield pa.RecordBatch.from_pylist(
                [{field: row[field] for field in required}]
            )

    monkeypatch.setattr(
        operator_adapters,
        "_columnar_request_dataset",
        lambda *_args: (FakeDataset(), SimpleNamespace(), plan),
    )

    with pytest.raises(ValueError, match="缺少研究窗口内可还原的修订前版本"):
        _temporal_corporate_action_rows(
            object(),
            "etf_corporate_actions",
            instrument_id="510300.XSHG",
            asset_class="cn_etf",
            trading_sessions=tuple(date(2024, 1, day) for day in (2, 3, 4, 5)),
            applicable_start=datetime.fromisoformat("2024-01-02T09:31:00+08:00"),
            applicable_end=datetime.fromisoformat("2024-01-05T15:01:00+08:00"),
            as_of=datetime.fromisoformat("2024-01-05T15:01:00+08:00"),
        )


@pytest.mark.parametrize("amount_type", [str, Decimal])
def test_stock_action_current_event_preserves_exact_amount(monkeypatch, amount_type) -> None:
    from research_pipeline.runtime.adapters import minute_adjustment as operator_adapters

    plan, dataset, original = _stock_action_history_fixture(
        revised_at=datetime(2024, 1, 2, 8)
    )
    original["fld_ca_cash_per_ten"] = amount_type("1.0")
    monkeypatch.setattr(
        operator_adapters,
        "_columnar_request_dataset",
        lambda *_args: (dataset, SimpleNamespace(), plan),
    )

    def rows_at(_context, _request_id, *, consumer_time, filters=()):
        assert len(filters) == 1
        assert filters[0].field == "fld_ca_code"
        assert filters[0].operator == "eq"
        assert filters[0].value == "000001.XSHE"
        return (dict(original),), SimpleNamespace(), plan

    monkeypatch.setattr(operator_adapters, "_columnar_request_rows_at", rows_at)
    rows, _reference = _temporal_corporate_action_rows(
        object(),
        "stock_corporate_actions",
        instrument_id="000001.XSHE",
        asset_class="cn_stock",
        trading_sessions=tuple(date(2024, 1, day) for day in (2, 3, 4, 5)),
        applicable_start=datetime.fromisoformat("2024-01-02T09:31:00+08:00"),
        applicable_end=datetime.fromisoformat("2024-01-05T15:01:00+08:00"),
        as_of=datetime.fromisoformat("2024-01-05T15:01:00+08:00"),
    )

    assert [row["__revision"] for row in rows] == [1]
    assert [row["fld_ca_cash_per_ten"] for row in rows] == [amount_type("1.0")]


def test_runtime_pre_feature_and_label_route_post_bars_per_selection_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    zone = datetime.fromisoformat("2024-01-04T09:31:00+08:00").tzinfo
    action = CorporateAction(
        "late:cash",
        1,
        "cash_dividend",
        "a" * 64,
        datetime(2024, 1, 4, 9, 15, tzinfo=zone),
        date(2024, 1, 2),
        date(2024, 1, 3),
        date(2024, 1, 3),
        date(2024, 1, 3),
        cash_per_share_microunits=1_000_000,
    )
    snapshot = build_adjustment_factor_snapshot(
        instrument_id="000001.XSHE",
        asset_class="cn_stock",
        as_of=datetime(2024, 1, 4, 15, tzinfo=zone),
        applicable_start=datetime(2024, 1, 2, 9, 31, tzinfo=zone),
        applicable_end=datetime(2024, 1, 5, 0, tzinfo=zone),
        initial_factor_date=date(2023, 12, 29),
        initial_factor=Decimal("1"),
        source_revision_hash="1" * 64,
        corporate_actions=(action,),
        effective_times={
            date(2024, 1, 3): datetime(2024, 1, 3, 9, 31, tzinfo=zone)
        },
        previous_closes={date(2024, 1, 3): Decimal("10")},
    )
    effective = {"2024-01-03": "2024-01-03T09:31:00+08:00"}
    closes = {"2024-01-03": "10"}
    source_bars_hash = "2" * 64
    source_hash = typed_canonical_hash({
        "bars": source_bars_hash,
        "adjustment_snapshot": snapshot.snapshot_identity_hash,
        "adjustment_candidates": [action.to_dict()],
        "adjustment_effective_times": effective,
        "adjustment_previous_closes": closes,
    })
    payload = {
        "price_mode": "post",
        "adjustment_snapshot": snapshot.to_dict(),
        "adjustment_snapshot_identity_hash": snapshot.snapshot_identity_hash,
        "adjustment_candidates": [action.to_dict()],
        "adjustment_effective_times": effective,
        "adjustment_previous_closes": closes,
        "source_bars_snapshot_hash": source_bars_hash,
        "source_snapshot_hash": source_hash,
    }

    def row(observed: datetime, session_id: str, close: float):
        return {
            "instrument": "000001.XSHE",
            "bar_start": observed - timedelta(minutes=1),
            "bar_end": observed,
            "available_time": observed,
            "session_id": session_id,
            "close": close,
            "open": close,
            "high": close,
            "low": close,
            "avg": close,
            "volume": 100.0,
            "money": close * 100,
            "factor": 1.0,
            "adjustment_ratio": 1.0,
            "adjustment_mode": "post",
            "factor_snapshot_hash": snapshot.snapshot_identity_hash,
            "completed": True,
            "quality_status": "pass",
            "source_snapshot_hash": source_hash,
            "source_partition_id": "fixture-partition",
        }

    rows = (
        row(datetime(2024, 1, 2, 15, tzinfo=zone), "2024-01-02", 10.0),
        row(datetime(2024, 1, 3, 9, 31, tzinfo=zone), "2024-01-03", 9.0),
        row(datetime(2024, 1, 4, 9, 31, tzinfo=zone), "2024-01-04", 9.2),
    )
    from research_pipeline.runtime.adapters import minute_research as operator_adapters

    monkeypatch.setattr(operator_adapters, "_input_external_payload", lambda *_: payload)
    monkeypatch.setattr(
        operator_adapters,
        "_operator_bar_partitions",
        lambda *_: (
            SimpleNamespace(),
            iter(((SimpleNamespace(partition_key="fixture"), iter(rows)),)),
        ),
    )

    def capture_partitions(_, *, partition_rows, **__):
        return {
            "rows": [
                dict(item)
                for _partition, output_rows in partition_rows
                for item in output_rows
            ]
        }

    monkeypatch.setattr(
        operator_adapters,
        "_commit_partitioned_minute_rows",
        capture_partitions,
    )
    monkeypatch.setattr(
        operator_adapters,
        "_environment",
        lambda _: SimpleNamespace(fixed_clock="2024-01-04T15:00:00+08:00"),
    )

    feature_parameters = {
        "feature_ids": ["lagged_return"],
        "lookback_bars": 2,
        "warmup_bars": 2,
        "gap_policy": "reset",
        "cross_session": True,
        "adjustment_mode": "pre",
        "asset_class": "cn_stock",
        "interval_minutes": 1,
        "availability_policy_ref": "pit.completed-bar.v1",
    }
    monkeypatch.setattr(operator_adapters, "_parameters", lambda _: feature_parameters)
    features = execute_research_features_intraday_v1(object())
    assert [item["adjustment_anchor_factor"] for item in features["rows"]] == [
        1.0,
        pytest.approx(10 / 9),
    ]

    label_parameters = {
        "entry_event": "bar_close",
        "exit_event": "bar_close",
        "horizon_bars": 1,
        "overlap": True,
        "adjustment_mode": "pre",
        "asset_class": "cn_stock",
        "interval_minutes": 1,
        "availability_policy_ref": "pit.label-horizon.v1",
    }
    monkeypatch.setattr(operator_adapters, "_parameters", lambda _: label_parameters)
    labels = execute_research_labels_intraday_v1(object())
    assert [item["adjustment_anchor_factor"] for item in labels["rows"]] == [
        1.0,
        pytest.approx(10 / 9),
    ]


def _short_package(
    tmp_path: Path,
    name: str,
    *,
    trial_candidate_ids: tuple[str, ...] | None = None,
    sample_count: int = 6,
) -> tuple[object, object]:
    source = REFERENCES / name
    target = tmp_path / name
    shutil.copytree(source, target)
    prepare_observation_package(target)
    spec_path = target / "spec/research.yaml"
    payload = yaml.safe_load(spec_path.read_text(encoding="utf-8"))
    future = name == "minute_futures_night_session"
    start = "2024-01-02T21:01:00+08:00" if future else "2024-01-02T09:31:00+08:00"
    start_at = datetime.fromisoformat(start)
    end_at = start_at + timedelta(minutes=sample_count)
    visible_at = end_at - timedelta(minutes=1)
    visible = visible_at.isoformat()
    payload["as_of"] = visible
    request_counts = {}
    for request in payload["requests"]:
        if not request["dataset_id"].endswith("minute_bar"):
            continue
        request_count = (
            max(sample_count - 1, 1)
            if request["request_id"].endswith("_decision")
            else sample_count
        )
        request_counts[request["request_id"]] = request_count
        request["time_range"]["start_at"] = start
        request["time_range"]["end_at"] = (
            start_at + timedelta(minutes=request_count)
        ).isoformat()
        request["budget"] = {
            "max_rows": request_count,
            "max_bytes": 1_000_000,
            "batch_size": min(request_count, 20),
        }
        request["limit"] = request_count
    for node in payload["graph"]["nodes"]:
        parameters = node["parameters"]
        if node["operator_id"] == "data.minute.scan":
            parameters["max_returned_rows"] = request_counts[
                parameters["request_ids"][0]
            ]
            parameters["max_batch_bytes"] = 1_000_000
        elif node["operator_id"] == "research.bars.minute_resample":
            parameters["interval_minutes"] = 1
        elif node["operator_id"] == "research.features.intraday":
            parameters["interval_minutes"] = 1
            parameters["lookback_bars"] = 2
            parameters["warmup_bars"] = 2
        elif node["operator_id"] == "research.labels.intraday":
            parameters["interval_minutes"] = 1
            parameters["horizon_bars"] = 1
            parameters["overlap"] = True
        elif (
            node["operator_id"] == "research.statistics.minute"
            and trial_candidate_ids is not None
        ):
            parameters["trial_candidate_ids"] = list(trial_candidate_ids)
        if node["operator_id"] == "research.statistics.minute" and sample_count >= 40:
            parameters["train_end_ns"] = int(
                (start_at - timedelta(minutes=3)).timestamp() * 1_000_000_000
            )
            parameters["validation_end_ns"] = int(
                (start_at - timedelta(minutes=2)).timestamp() * 1_000_000_000
            )
            parameters["test_end_ns"] = int(
                (end_at + timedelta(minutes=1)).timestamp() * 1_000_000_000
            )
            parameters["embargo_ns"] = 60_000_000_000
    spec_path.write_text(
        yaml.safe_dump(payload, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    package = load_research_package(target)
    registry = build_mainline_operator_registry()
    return package, compile_research_package(package, admission=registry)


def _admit(plan: object, *, request_id: str | None = None) -> object:
    query = (
        plan.queries[0]
        if request_id is None
        else plan.queries[plan.request_ids.index(request_id)]
    )
    catalog = CompiledCatalog.load_default()
    binding = next(
        item
        for item in catalog.bindings.values()
        if item["dataset_id"] == query.dataset_id
    )
    drift = PolicyContract(**dict(catalog.policies[binding["drift_policy_id"]]))
    attestation = DriftAttestation(
        catalog.catalog_hash,
        binding["binding_id"],
        binding["source_profile"],
        binding["environment"],
        binding["expected_schema_revision"],
        binding["expected_schema_revision"],
        drift.content_hash,
        True,
    )
    return admit_query(
        query,
        catalog=catalog,
        binding=binding,
        attestation=attestation,
    )


def _admit_all(plan: object) -> dict[str, object]:
    return {
        request_id: _admit(plan, request_id=request_id)
        for request_id in plan.request_ids
    }


def _minute_source(root: Path, *, future: bool, sample_count: int = 6) -> Path:
    source = root / ("futures" if future else "stock") / "year=2024/month=01"
    source.mkdir(parents=True)
    first = datetime(2024, 1, 2, 21, 1) if future else datetime(2024, 1, 2, 9, 31)
    code = "AG2406.XSGE" if future else "000001.XSHE"
    rows = []
    for offset in range(sample_count):
        price = (
            6000.0 + min(offset, sample_count - offset)
            if future
            else 10.0 + offset * 0.1
        )
        high = price + (1.0 if future else 0.1)
        low = price - (1.0 if future else 0.1)
        close = price if future else price + 0.05
        avg = price if future else price + 0.02
        row = {
            "code": code,
            "dt": first + timedelta(minutes=offset),
            "open": price,
            "high": high,
            "low": low,
            "close": close,
            "volume": 1_000.0,
            "money": 10_000.0 + offset,
            "avg": avg,
        }
        if future:
            row["open_interest"] = 50_000.0 + offset
        rows.append(row)
    fields = [
        ("code", pa.string()),
        ("dt", pa.timestamp("us")),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("volume", pa.float64()),
        ("money", pa.float64()),
        ("avg", pa.float64()),
    ]
    if future:
        fields.append(("open_interest", pa.float64()))
    pq.write_table(pa.Table.from_pylist(rows, schema=pa.schema(fields)), source / "data.parquet")
    return source / "data.parquet"


def _environment(
    tmp_path: Path,
    *,
    package: object,
    plan: object,
    admitted: object,
    minute_root: Path,
) -> ResearchRunEnvironment:
    parameters = {
        node.node_id: dict(node.parameters)
        for node in plan.recipe.nodes
    }
    complete_admission = isinstance(admitted, dict)
    admitted_plans = (
        admitted
        if complete_admission
        else {plan.request_ids[0]: admitted}
    )
    consumed_request_ids = (
        _formal_input_lineage(
            recipe=plan.recipe,
            result_spec=plan.result_spec,
            admitted_request_ids=frozenset(admitted_plans),
        )[1]
        if complete_admission
        else tuple(sorted(admitted_plans))
    )
    database = tmp_path / "unused.duckdb"
    database.touch(exist_ok=True)
    return ResearchRunEnvironment(
        plan_root=tmp_path / "plan",
        manifest={
            "package_plan_hash": plan.plan_hash,
            "package_id": package.package_id,
            "consumed_request_ids": list(consumed_request_ids),
            "input_claim_ceilings": {
                request_id: admitted_plans[request_id].input_claim_ceiling
                for request_id in consumed_request_ids
            },
        },
        admitted_plans=admitted_plans,
        database=database,
        source_databases={},
        minute_data_root=minute_root,
        holdout_ledger_anchor=tmp_path / "artifacts",
        node_parameters=parameters,
        semantics=None,
        fixed_clock=plan.fixed_clock,
        root_seed=plan.root_seed,
        workers=1,
        resource_timeout_seconds=None,
        strategy_spec_hashes={"weighting.positive_equal_cash": "a" * 64},

    )


def _service() -> RuntimeExecutionService:
    audit = AuditEnvironmentManifest.capture(
        build_artifact_digest="a" * 64,
        dependency_distribution_digests={"runtime": "b" * 64},
    )
    return RuntimeExecutionService(
        audit_environment=audit,
        numerical_backend_names=("runtime",),
    )


def test_formal_minute_observation_chain_publishes_result_metric(tmp_path: Path) -> None:
    package, plan = _short_package(
        tmp_path,
        "minute_futures_night_session",
        sample_count=40,
    )
    admitted_plans = _admit_all(plan)
    minute_root = tmp_path / "minute-data"
    source_file = _minute_source(minute_root, future=True, sample_count=40)
    registry = build_mainline_operator_registry()
    dag = compile_operator_graph_dag(plan, registry)
    run_root = tmp_path / "run"
    environment = _environment(
        tmp_path,
        package=package,
        plan=plan,
        admitted=admitted_plans,
        minute_root=minute_root,
    )

    result = _service().execute(
        dag=dag,
        environment=environment,
        run_root=run_root,
        project_id=package.package_id,
        root_seed=plan.root_seed,
        fixed_clock=plan.fixed_clock,
        mode=ExecutionMode.DETERMINISTIC_SERIAL,
    )

    assert result["status"] == "succeeded"
    assert set(result["outputs"]) == {node.node_id for node in dag.nodes}
    observation_key = result["outputs"]["observe_futures"]["observation"][
        "artifact_key"
    ]
    observation_table = (
        run_root
        / "external-artifacts/objects"
        / observation_key
        / "observation/part-00000.parquet"
    )
    assert observation_table.is_file()
    published_observation = pq.read_table(observation_table)
    assert published_observation.column_names == [
        "metric_ref",
        "value",
        "unit",
        "sample_start",
        "sample_end",
        "sample_size",
        "status",
    ]
    assert published_observation.num_rows == 1
    assert published_observation.column("metric_ref").to_pylist() == [
        "minute.row_count@1.0.0"
    ]
    assert published_observation.column("status").to_pylist() == ["computed"]

    data_bundle = _runtime_data_bundle(result, run_root)
    assert set(data_bundle["references"]) == set(plan.request_ids)
    reference = data_bundle["references"][plan.request_ids[0]]
    assert reference["contract_version"] == "partitioned-dataset-ref"
    assert len(reference["partitions"]) == 1
    partition = reference["partitions"][0]
    assert partition["root_role"] == "minute_data"
    assert minute_root / Path(partition["relative_path"]) == source_file
    assert not (run_root / Path(partition["relative_path"])).exists()
    result_store = ResultStore(tmp_path / "result-store")
    result_bundle, result_path, _ = ResultAssembler(result_store.root).finalize(
        run_root=run_root,
        project_id=package.package_id,
        package_hash=package.package_hash,
        plan_hash=plan.plan_hash,
        result_spec=plan.result_spec,
        catalog_hashes={
            request_id: item.catalog_hash
            for request_id, item in admitted_plans.items()
        },
        data_references=data_bundle["references"],
        metric_proofs=plan.metric_proofs,
        implementation_manifest_hash=build_mainline_operator_manifest().manifest_hash,
        claim_level="research_observation",
        formal_input_request_ids=tuple(sorted(data_bundle["references"])),
        input_claim_ceilings={key: "research_observation" for key in data_bundle["references"]},
    )
    input_revision = result_bundle.input_revisions[0]
    assert input_revision.physical_snapshot_id == PartitionedDatasetRef.from_dict(
        reference
    ).reference_id
    assert input_revision.manifest_hash == input_revision.physical_snapshot_id
    observation_manifest = next(
        table for table in result_bundle.tables if table.table_id == "futures_observation"
    )
    assert observation_manifest.role == "primary"
    assert sum(observation_manifest.row_counts.values()) == 1
    assert {
        table.table_id for table in result_bundle.tables if table.role == "diagnostic"
    } >= {"minute_features"}
    _, metrics = load_result_metrics(result_store, result_bundle)
    assert [metric.metric_ref for metric in metrics] == [
        "minute.row_count@1.0.0"
    ]
    assert metrics[0].status == "computed"
    assert result_bundle.claim_level == "research_observation"
    assert not (tmp_path / "handoff.json").exists()


def _observation_context(
    tmp_path: Path,
    *,
    bars_request_id: str = "futures_minute",
) -> OperatorRuntimeContext:
    package, plan = _short_package(
        tmp_path, "minute_futures_night_session", sample_count=10
    )
    admitted = _admit(plan)
    lineage = {"source": "two-partition-test"}
    start = datetime.fromisoformat("2024-01-02T21:01:00+08:00")
    partitions = tuple(
        DatasetPartitionRef(
            partition_key=f"part-{index}",
            logical_start=start + timedelta(minutes=offset),
            logical_end=start + timedelta(minutes=offset + count),
            root_role="runtime_artifact",
            relative_path=f"minute/part-{index}.parquet",
            source_kind="runtime_derived",
            size=1,
            mtime_ns=1,
            row_count=count,
            row_groups=1,
            schema_hash="c" * 64,
            sort_keys=("code", "dt"),
            lineage=lineage,
        )
        for index, (offset, count) in enumerate(((0, 3), (3, 7)), start=1)
    )
    dataset = PartitionedDatasetRef(
        dataset_id="minute/test-observation",
        timestamp_field="dt",
        instrument_field="code",
        instruments=("AG2406.XSGE",),
        allowed_columns=("code", "dt"),
        partitions=partitions,
        lineage=lineage,
    )
    store = ExternalArtifactStore(tmp_path / "external")
    staging = store.prepare()
    (staging / "result.json").write_text(
        canonical_json({
            "request_id": bars_request_id,
            "partitioned_dataset": dataset.to_dict(),
        }),
        encoding="utf-8",
    )
    commit = store.commit(
        staging,
        artifact_name="bars",
        artifact_type="data.minute-bars.v1",
    )
    dag = compile_operator_graph_dag(plan, build_mainline_operator_registry())
    node = next(item for item in dag.nodes if item.node_id == "observe_futures")
    work_dir = tmp_path / "work"
    work_dir.mkdir()
    node_context = RuntimeNodeContext(
        node,
        {"bars": RuntimeNodeValue.external(commit)},
        work_dir,
        store,
        plan.root_seed,
        plan.fixed_clock,
        node.resource_budget,
        None,
        None,
    )
    environment = _environment(
        tmp_path,
        package=package,
        plan=plan,
        admitted=admitted,
        minute_root=tmp_path / "unused-minute-data",
    )
    return OperatorRuntimeContext(node_context, environment)


def test_minute_observation_sums_partition_manifest_without_reading_price_rows(
    tmp_path: Path,
) -> None:
    outputs = execute_research_observation_minute_bars_v1(
        _observation_context(tmp_path)
    )
    value = outputs.values["observation"]
    assert value.external_commit is not None
    root = (
        tmp_path
        / "external/objects"
        / value.external_commit.semantic_hash
    )
    payload = json.loads((root / "result.json").read_text(encoding="utf-8"))
    metrics = pq.read_table(root / "observation/part-00000.parquet").to_pylist()

    assert payload["partition_count"] == 2
    assert payload["row_count"] == 10
    assert metrics[0]["value"] == 10.0
    assert metrics[0]["sample_size"] == 10
    assert outputs.completion_metadata is not None
    assert outputs.completion_metadata.counts["observed_row_count"] == 10


def test_minute_observation_rejects_request_id_mismatch(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="请求与 bars 来源不一致"):
        execute_research_observation_minute_bars_v1(
            _observation_context(tmp_path, bars_request_id="other_request")
        )


def test_resample_rejects_source_revision_drift_before_using_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    package, plan = _short_package(tmp_path, "minute_futures_night_session")
    admitted = _admit_all(plan)
    minute_root = tmp_path / "minute-data"
    source_file = _minute_source(minute_root, future=True)
    dag = compile_operator_graph_dag(plan, build_mainline_operator_registry())
    environment = _environment(
        tmp_path,
        package=package,
        plan=plan,
        admitted=admitted,
        minute_root=minute_root,
    )
    from research_pipeline.runtime.adapters import minute_data as operator_adapters

    original = operator_adapters.execute_research_bars_minute_resample_v1

    def drift_then_resample(context):
        if context.node_context.node.node_id == "bars_1m":
            source_file.touch()
        return original(context)

    monkeypatch.setattr(
        operator_adapters,
        "execute_research_bars_minute_resample_v1",
        drift_then_resample,
    )
    with pytest.raises(RuntimeWorkerError, match="bars_1m") as error:
        _service().execute(
            dag=dag,
            environment=environment,
            run_root=tmp_path / "run",
            project_id=package.package_id,
            root_seed=plan.root_seed,
            fixed_clock=plan.fixed_clock,
        )
    assert "物理源发生漂移" in str(error.value.__cause__)


def test_raw_stock_source_passes_formal_admission(
    tmp_path: Path,
) -> None:
    target = tmp_path / "stock"
    shutil.copytree(REFERENCES / "minute_stock_session", target)
    prepare_observation_package(target)
    package = load_research_package(target)
    plan = compile_research_package(
        package, admission=build_mainline_operator_registry()
    )
    admitted = _admit(plan, request_id="stock_minute")
    assert admitted.query.adjustment == "raw"


def test_pre_adjustment_package_must_archive_each_consumed_anchor_table(
    tmp_path: Path,
) -> None:
    target = tmp_path / "minute_stock_session"
    shutil.copytree(REFERENCES / "minute_stock_session", target)
    prepare_observation_package(target)
    spec_path = target / "spec/research.yaml"
    payload = yaml.safe_load(spec_path.read_text(encoding="utf-8"))
    payload["result"]["tables"] = [
        item
        for item in payload["result"]["tables"]
        if item["table_id"] != "adjustment_label_anchors"
    ]
    spec_path.write_text(
        yaml.safe_dump(payload, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )

    package = load_research_package(target)
    with pytest.raises(ResearchPackageError, match="逐决策锚点行"):
        compile_research_package(
            package,
            admission=build_mainline_operator_registry(),
        )


def test_adjusted_package_must_archive_the_consumed_snapshot(
    tmp_path: Path,
) -> None:
    target = tmp_path / "minute_stock_session"
    shutil.copytree(REFERENCES / "minute_stock_session", target)
    prepare_observation_package(target)
    spec_path = target / "spec/research.yaml"
    payload = yaml.safe_load(spec_path.read_text(encoding="utf-8"))
    payload["result"]["tables"] = [
        item
        for item in payload["result"]["tables"]
        if item["table_id"] != "adjustment_snapshot"
    ]
    spec_path.write_text(
        yaml.safe_dump(payload, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )

    package = load_research_package(target)
    with pytest.raises(ResearchPackageError, match="实际快照"):
        compile_research_package(
            package,
            admission=build_mainline_operator_registry(),
        )
