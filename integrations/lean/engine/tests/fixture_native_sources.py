"""将合成源写成正式 Parquet 工件和冻结数据绑定。"""

from datetime import date, datetime, timedelta
import json

import pyarrow as pa
import pyarrow.parquet as pq

from research_pipeline.data_plane import (
    AdmittedQueryPlan, DateRangeV1, QueryBudget, QueryIR, QueryPurpose, SortKey,
    UniverseSelection, build_dataset_artifact_ref, build_logical_snapshot, publish_parquet_snapshot,
)
from research_pipeline.data_plane.revision import SourceFileEvidence, SourceRevision
from research_pipeline.data_plane.temporal import SelectionClock, TemporalSelectionPlan, VisibilityFilter
from research_pipeline.domain.lean_algorithm import LeanDataBinding
from research_pipeline.domain.trading import InstrumentKey, PortfolioTarget, PortfolioTargetEntry
from research_pipeline.runtime.lean_native_direct import direct_source


MEMORY = 64 * 1024 * 1024
LINEAGE = 'a' * 64
DATE = (pa.date32(), 'date32')
TEXT = (pa.string(), 'string')
NUMBER = (pa.float64(), 'float64')


def _archive(root, alias, symbols, days, schema, rows, *, visibility=None):
    fields = tuple(schema)
    types = tuple((name, value[1]) for name, value in schema.items())
    arrow = pa.schema([(name, value[0]) for name, value in schema.items()])
    table = pa.Table.from_pylist(sorted(rows, key=lambda row: (row['date'], row['code'])), schema=arrow)
    start, end = map(date.fromisoformat, (days[0], days[-1]))
    query = QueryIR(alias, 1, fields, QueryPurpose.FEATURE, DateRangeV1(start, end),
                    UniverseSelection(tuple(symbols)), (), (SortKey('date'), SortKey('code')),
                    QueryBudget(1000, MEMORY, 4096))
    temporal = TemporalSelectionPlan(SelectionClock('consumer_observation_time' if visibility else 'event_time'),
        'date', fields, fields, types, source_timezone='Asia/Shanghai', visibility_filter=visibility)
    plan = AdmittedQueryPlan(query=query, catalog_hash=LINEAGE, binding_id=alias, binding_version=1,
        attestation_hash=LINEAGE, source_profile='synthetic', environment='test', expected_schema_revision=LINEAGE,
        availability_policy_hash=LINEAGE, revision_policy_hash=LINEAGE, object_name=alias,
        columns=tuple((name, name) for name in fields), field_types=types,
        field_nullables=tuple((name, False) for name in fields), primary_key=('date', 'code'),
        event_time_field='date', instrument_field='code', temporal_selection=temporal,
        input_claim_ceiling='research_observation')
    revision = SourceRevision(alias, 'parquet', (SourceFileEvidence('synthetic.arrow', 0, 0),),
                              LINEAGE, end.isoformat(), 'metadata')
    logical = build_logical_snapshot(plan, source_revisions=(revision,), output_schema=arrow,
                                     provider_version='synthetic-arrow-v1', compiler_version='synthetic-v1')
    snapshot = publish_parquet_snapshot(batches=table.to_batches(), schema=arrow, plan=plan,
        logical_snapshot=logical, root=root, temporal_source=temporal.requires_consumer_binding)
    return plan, build_dataset_artifact_ref(snapshot, allowed_root=root).to_dict()


def daily_source(root, binding, days, rows, *, futures=False):
    tail_session = (date.fromisoformat(days[-1]) + timedelta(days=1)).isoformat()
    clocks = {'rule': 'next_session_open', 'timezone': 'Asia/Shanghai', 'sessions': [*days, tail_session]}
    fields = ('open', 'high', 'low', 'close', 'volume')
    prices = [dict(date=row['time'].date(), code=row['symbol'], **{name: row[name] for name in fields})
              for row in rows if not row.get('execution_open')]
    opens = [dict(date=row['time'].date(), code=row['symbol'], open=row['open'])
             for row in rows if row.get('execution_open')]
    schema = dict(date=DATE, code=TEXT, **dict.fromkeys(fields, NUMBER))
    plan, reference = _archive(root, 'cn_futures.daily_bar' if futures else 'bars', binding['symbols'], days, schema, prices)
    open_plan, open_ref = _archive(root, 'open', binding['symbols'], days, dict(date=DATE, code=TEXT, open=NUMBER), opens)
    source = LeanDataBinding(binding_id='bars', input_port='bars', request_id='bars', kind='bars',
                             resolution='1d', columns=tuple(schema), daily_time=clocks)
    descriptor = direct_source(root, reference, source, plan, MEMORY, temp_bytes=MEMORY)
    descriptor['opening'] = dict(reference=open_ref, plan=open_plan.to_dict(), column='open', days=list(days))
    return {**binding, 'direct': descriptor}


def universe_target_sources(root, binding, days):
    code = binding['symbols'][0]
    hold = ('history_start_at', 'start_at', 'end_at')

    def declaration(alias, kind, direct):
        return dict(binding_id=alias, kind=kind, resolution='event', symbols=[code], trade_symbols=[code],
                    page_size=1, direct=direct, **{name: binding[name] for name in hold})

    # 周末公告沿冻结日历在下个交易日开盘可见。
    selection = ('selection', 'universe', 'session_open', dict(selected=(pa.bool_(), 'bool')),
                 [dict(date=date.fromisoformat(day), code=code, selected=index == 0)
                  for index, day in enumerate((days[0], days[2]))], None)
    announcements = ('announcements', 'event', 'next_session_open_following_publication_date',
                     dict(revision=(pa.int64(), 'int64')),
                     [dict(date=date(2021, 10, 9), code=code, revision=1)], VisibilityFilter('date', True))
    probe = ('publication_probe', 'event', 'business_effective_date', {},
             [dict(date=date.fromisoformat(days[1]), code=code)], VisibilityFilter('date', True))
    bindings = []
    for alias, kind, rule, fields, raw, visibility in (selection, announcements, probe):
        schema = dict(date=DATE, code=TEXT, **fields)
        plan, reference = _archive(root, alias, [code], days, schema, raw, visibility=visibility)
        source = LeanDataBinding(binding_id=alias, input_port=alias, request_id=alias, kind=kind, resolution='event',
            columns=tuple(schema), daily_time=dict(rule=rule, timezone='Asia/Shanghai', sessions=list(days)))
        bindings.append(declaration(alias, kind, direct_source(root, reference, source, plan, MEMORY, temp_bytes=MEMORY)))

    instrument = InstrumentKey(code, 'cn_stock', 'XSHE', 'CNY', 'stock')
    records = []
    for observed, execution, weight in zip(days, days[1:], (0.24, 0.0)):
        decision = datetime.fromisoformat(observed + 'T15:00:00+08:00')
        target = PortfolioTarget(decision, 'weight', (PortfolioTargetEntry(instrument, 'weight', weight),),
                                 'CNY', 1 - weight, False, 1.0, (LINEAGE,))
        records.append(dict(decision_time=decision, order_time=datetime.fromisoformat(execution + 'T09:30:00+08:00'),
                            target_json=json.dumps(target.to_dict())))
    filename = 'targets.parquet'
    pq.write_table(pa.Table.from_pylist(records), root / filename)
    bindings.append(declaration('portfolio_targets', 'targets', dict(kind='targets', root=str(root), files=[filename],
                                memory_bytes=MEMORY, instruments=[instrument.to_dict()])))
    return bindings
