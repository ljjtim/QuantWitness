"""causal Feature到策略signals的文件接线与可见时间约束；不连接数据库。"""
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
import json
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from research_pipeline.domain import InstrumentKey
from research_pipeline.domain.strategy_protocol import StrategyDeclaration, StrategyLimits, StrategyInputBinding
from research_pipeline.runtime.strategy_signal_inputs import StrategySignalInputs, open_strategy_signal_inputs, signal_payload
from research_pipeline.runtime.external_artifact import ExternalArtifactStore


A = InstrumentKey("000001.XSHE", "cn_stock", "XSHE", "CNY", "stock")


def _dt(day, hour, minute):
    return datetime(2024, 1, day, hour, minute, tzinfo=timezone(timedelta(hours=8)))


def declaration(**changes):
    return StrategyDeclaration(**{**dict(strategy_id="test", bundle_id="test", state_schema_id="test-state-v1",
        subscriptions=("initialize", "before_trading", "execution_feedback", "bar", "after_trading"), schedules=(),
        input_bindings=(StrategyInputBinding(binding_id="quotes", columns=("close", "volume"), lookback_bars=2),),
        limits=StrategyLimits(max_state_bytes=4096, max_context_bytes=65536, max_response_bytes=65536,
                             max_intents_per_event=20, callback_wall_seconds=3)), **changes})


def selected_declaration(**changes):
    base = declaration(bundle_id="b" * 64)
    bindings = (*base.input_bindings, StrategyInputBinding(binding_id="selection", source="signals",
        columns=("rank", "candidate_count"), lookback_bars=1))
    return replace(base, input_bindings=bindings, **changes)


def feature_rows(day=3, *, count=1, hour=9, minute=30):
    at = _dt(day, hour, minute)
    return [dict(event_id=f"2024-01-{day:02d}:{slot}", entity_id=A.instrument_id if slot and slot <= count else None,
        rank=slot if slot <= count else 0, candidate_count=count,
        decision_time=at, max_source_observation_time=at - timedelta(minutes=1),
        max_source_available_time=at, source_partition_ids=["snapshot:2024-01"])
        for slot in range(3)]


class SignalSink:
    def __init__(self):
        self.rows = []

    def signal(self, row):
        self.rows.append(row)


def provider(rows, declared=None):
    return StrategySignalInputs(iter(rows), bindings=(declared or selected_declaration()).input_bindings,
        instruments=(A,), max_context_bytes=65536)


def source_context(tmp_path, rows, *, row_group_size=None, time_unit=None):
    store = ExternalArtifactStore(tmp_path / "source-store")
    staging = store.prepare()
    target = staging / "features/data.parquet"
    target.parent.mkdir()
    table = pa.Table.from_pylist(rows)
    if time_unit is not None:
        table = table.cast(pa.schema([
            pa.field(field.name, pa.timestamp(time_unit, field.type.tz))
            if pa.types.is_timestamp(field.type) else field for field in table.schema
        ]))
    pq.write_table(table, target, row_group_size=row_group_size)
    committed = store.commit(staging, artifact_name="features", artifact_type="research.feature-set.v1")
    work = tmp_path / "source-work"
    work.mkdir()
    context = SimpleNamespace(inputs={"signals": SimpleNamespace(external_commit=committed)}, external_store=store,
        work_dir=work, effective_resource_budget=SimpleNamespace(memory_bytes=256*1024**2, temp_bytes=8*1024**2))
    return context, committed


def test_signal_release_uses_core_decision_and_keeps_complete_nullable_cohort():
    first, future = feature_rows(), feature_rows(day=4, count=0)
    values = provider(first + future)
    assert values.visible(_dt(3, 9, 29)) == ()
    visible = values.visible(_dt(3, 9, 30))
    assert len(visible) == 3
    assert visible[0].instrument is None and visible[0].price is None
    assert visible[1].instrument == A
    assert all(item.bar_start is None and item.available_at == _dt(3, 9, 30) for item in visible)
    assert len(values.visible(_dt(3, 15, 0))) == 3
    next_day = values.visible(_dt(4, 9, 30))
    assert len(next_day) == 3 and all(item.instrument is None for item in next_day)
    assert all(dict(item.values)["candidate_count"] == 0 for item in next_day)
    assert values.history.maxlen == len(values.history) == 1


def test_signal_lookback_counts_decision_cohorts_not_null_instruments():
    base = selected_declaration()
    bindings = (replace(base.input_bindings[-1], lookback_bars=2),)
    declared = replace(base, input_bindings=bindings)
    values = provider(feature_rows() + feature_rows(day=4, count=0), declared)
    assert len(values.visible(_dt(4, 9, 30))) == 6


@pytest.mark.parametrize("mutation", ["entity", "float", "boolean", "duplicate", "budget", "clock"])
def test_signal_window_rejects_invalid_values_and_budget(mutation):
    rows = feature_rows()
    if mutation == "entity":
        rows[1]["entity_id"] = "000009.XSHE"
    elif mutation == "float":
        rows[1]["rank"] = 1.5
    elif mutation == "boolean":
        rows[1]["rank"] = True
    elif mutation == "duplicate":
        rows[1]["event_id"] = rows[0]["event_id"]
    values = provider(rows)
    if mutation == "budget":
        values.max_context_bytes = 10
    if mutation == "clock":
        values.visible(_dt(3, 9, 30))
        with pytest.raises(ValueError, match="倒退"):
            values.visible(_dt(3, 9, 29))
    else:
        with pytest.raises(ValueError):
            values.visible(_dt(3, 9, 30))


def test_formal_feature_file_sorts_and_seals_all_original_rows(tmp_path):
    rows = feature_rows(day=4) + feature_rows()
    context, commit = source_context(tmp_path, rows)
    sink = SignalSink()
    with open_strategy_signal_inputs(context, declaration=selected_declaration(), instruments=(A,), evidence_sink=sink) as signals:
        assert signals.source_artifact_key == commit.semantic_hash
        assert len(sink.rows) == 6
        assert len(signals.visible(_dt(3, 9, 30))) == 3
        assert all(item.input_id.startswith("selection:signal:2024-01-03:") for item in signals.visible(_dt(3, 15, 0)))
    assert sink.rows == [signal_payload(row) for row in rows]
    assert not (context.work_dir / "strategy-signal-sort").exists()


@pytest.mark.parametrize("mutation", ["lineage", "future", "schema", "float", "entity", "missing"])
def test_formal_feature_file_rejects_non_causal_or_unbound_rows(tmp_path, mutation):
    rows = feature_rows()
    if mutation == "lineage":
        rows[0]["source_partition_ids"] = []
    elif mutation == "future":
        rows[0]["max_source_available_time"] += timedelta(minutes=1)
    elif mutation == "schema":
        for row in rows:
            row["rank"] = str(row["rank"])
    elif mutation == "float":
        for row in rows:
            row["rank"] = float(row["rank"])
    elif mutation == "entity":
        rows[1]["entity_id"] = "000009.XSHE"
    elif mutation == "missing":
        for row in rows:
            del row["decision_time"]
    context, _ = source_context(tmp_path, rows)
    with pytest.raises(ValueError):
        with open_strategy_signal_inputs(context, declaration=selected_declaration(), instruments=(A,), evidence_sink=SignalSink()):
            pass
    assert not (context.work_dir / "strategy-signal-sort").exists()


@pytest.mark.parametrize("time_unit", ["us", "ns"])
def test_day_filter_uses_local_midnight_and_skips_other_day_invalid_facts(tmp_path, monkeypatch, time_unit):
    import pyarrow.dataset as ds
    from research_pipeline.runtime import strategy_signal_inputs as module

    rows = []
    for day, hour, minute in ((2, 23, 59), (3, 0, 0), (3, 23, 59), (4, 0, 0)):
        cohort = feature_rows(day=day, hour=hour, minute=minute)
        for row in cohort:
            row["event_id"] += f":{hour}:{minute}"
            for name in ("decision_time", "max_source_observation_time", "max_source_available_time"):
                row[name] = row[name].astimezone(timezone.utc)
        if day != 3:
            cohort[0]["max_source_available_time"] += timedelta(days=2)
        rows.extend(cohort)
    context, _ = source_context(tmp_path, rows, row_group_size=3, time_unit=time_unit)
    before = {path.relative_to(tmp_path) for path in tmp_path.rglob("*")}
    validated = []
    original = module.validate_feature_time_facts

    def check(frame):
        validated.extend(frame["event_id"].tolist())
        original(frame)

    monkeypatch.setattr(module, "validate_feature_time_facts", check)
    original_dataset = ds.dataset
    scans = []

    class Dataset:
        def __init__(self, inner):
            self.inner = inner

        def scanner(self, **kwargs):
            assert kwargs["filter"] is not None
            assert "unused" not in kwargs["columns"]
            fragments = list(self.inner.get_fragments())
            groups = [group for fragment in fragments for group in
                      fragment.split_by_row_group(filter=kwargs["filter"], schema=self.inner.schema)]
            scans.append(sum(len(group.row_groups) for group in groups))
            return self.inner.scanner(**kwargs)

    monkeypatch.setattr(ds, "dataset", lambda *args, **kwargs: Dataset(original_dataset(*args, **kwargs)))
    sink = SignalSink()
    with open_strategy_signal_inputs(context, declaration=selected_declaration(), instruments=(A,),
                                     day=date(2024, 1, 3), evidence_sink=sink) as signals:
        assert len(signals.visible(_dt(3, 0, 0))) == 3
        assert len(signals.visible(_dt(3, 23, 59))) == 3
        assert signals.next_row is None
    assert scans == [2]
    assert len(validated) == len(sink.rows) == 6
    assert all("2024-01-03:" in event for event in validated)
    assert {path.relative_to(tmp_path) for path in tmp_path.rglob("*")} == before


@pytest.mark.parametrize("day", [None, date(2024, 1, 3)])
def test_signal_sort_budget_never_spills(tmp_path, day):
    rows = feature_rows() * 100
    for index, row in enumerate(rows):
        rows[index] = {**row, "event_id": str(index)}
    context, _ = source_context(tmp_path, rows)
    context.effective_resource_budget.memory_bytes = 1024
    before = {path.relative_to(tmp_path) for path in tmp_path.rglob("*")}
    with pytest.raises(ValueError, match="内存预算"):
        with open_strategy_signal_inputs(context, declaration=selected_declaration(), instruments=(A,), day=day):
            pass
    assert {path.relative_to(tmp_path) for path in tmp_path.rglob("*")} == before


def test_signal_empty_day_has_no_rows_or_files(tmp_path):
    context, _ = source_context(tmp_path, feature_rows())
    with open_strategy_signal_inputs(context, declaration=selected_declaration(), instruments=(A,),
                                     day=date(2024, 1, 4)) as signals:
        assert signals.next_row is None
        assert signals.visible(_dt(4, 9, 30)) == ()
    assert list(context.work_dir.iterdir()) == []


def test_day_signal_duplicate_across_row_groups_rejected(tmp_path):
    context, _ = source_context(tmp_path, feature_rows() * 2, row_group_size=3)
    with open_strategy_signal_inputs(context, declaration=selected_declaration(), instruments=(A,),
                                     day=date(2024, 1, 3)) as signals:
        with pytest.raises(ValueError, match="重复"):
            signals.visible(_dt(3, 9, 30))
    assert list(context.work_dir.iterdir()) == []


def test_cumulative_signal_memory_limit_across_batches(tmp_path, monkeypatch):
    from research_pipeline.runtime import strategy_signal_inputs as module

    base = feature_rows()[0]
    rows = [{**base, "event_id": str(index).zfill(5)} for index in range(1800)]
    table = pa.Table.from_pylist(rows)
    context, _ = source_context(tmp_path, rows, row_group_size=256)
    context.effective_resource_budget.memory_bytes = table.slice(0, 256).nbytes * 5 * 8
    validated = []
    original = module.validate_feature_time_facts

    def check(frame):
        validated.append(len(frame))
        original(frame)

    monkeypatch.setattr(module, "validate_feature_time_facts", check)
    before = {path.relative_to(tmp_path) for path in tmp_path.rglob("*")}
    with pytest.raises(ValueError, match="内存预算"):
        with open_strategy_signal_inputs(context, declaration=selected_declaration(), instruments=(A,)):
            pass
    assert 256 <= sum(validated) < len(rows)
    assert {path.relative_to(tmp_path) for path in tmp_path.rglob("*")} == before


def test_distinct_signal_windows_charge_only_delivered_projection():
    bindings=(StrategyInputBinding(binding_id="short",source="signals",columns=("rank",),lookback_bars=1),
              StrategyInputBinding(binding_id="long",source="signals",columns=("candidate_count",),lookback_bars=3))
    rows=[*feature_rows(day=2),*feature_rows(day=3),*feature_rows(day=4)]
    declared=replace(declaration(),input_bindings=bindings)
    generous=StrategySignalInputs(rows,bindings=bindings,instruments=(A,),max_context_bytes=100000)
    expected=generous.visible(_dt(4,9,31))
    from dataclasses import asdict
    used=sum(len(json.dumps(signal_payload(asdict(item)),ensure_ascii=False,separators=(",", ":")).encode("utf-8")) for item in expected)
    constrained=StrategySignalInputs(rows,bindings=bindings,instruments=(A,),max_context_bytes=used)
    assert constrained.visible(_dt(4,9,31))==expected
    assert len(expected)==12
