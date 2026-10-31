"""合成 ETF 日线执行、开盘状态与公司行动的独立来源合同。"""
from dataclasses import replace
import pyarrow as pa

from research_pipeline.catalog import DatasetContract, FieldContract, PhysicalBindingContract, PolicyContract
from research_pipeline.catalog.discovery import PhysicalColumn, PhysicalInventory


BARS_REQUEST = 'portfolio_bars'
OPEN_REQUEST = 'portfolio_open'
ACTION_REQUEST = 'portfolio_actions'
FACTOR_REQUEST = 'portfolio_factors'
BARS_FIELDS = ('fld_etf_date', 'fld_etf_code', 'fld_etf_open', 'fld_etf_high', 'fld_etf_low',
               'fld_etf_close', 'fld_etf_volume', 'fld_etf_factor')
OPEN_FIELDS = ('fld_etf_open_state_date', 'fld_etf_open_state_code', 'fld_etf_open_state_open')
ACTION_FIELDS = {
    'fld_etf_ca_id': ('corporate_action.etf.source_id', pa.int64(), 'identifier', 'dimensionless'),
    'fld_etf_ca_code': ('corporate_action.etf.instrument_id', pa.string(), 'identifier', 'dimensionless'),
    'fld_etf_ca_pub_date': ('corporate_action.etf.publication_date', pa.date32(), 'date', 'day'),
    'fld_etf_ca_event_id': ('corporate_action.etf.event_id', pa.int64(), 'identifier', 'dimensionless'),
    'fld_etf_ca_event': ('corporate_action.etf.event', pa.string(), 'category', 'dimensionless'),
    'fld_etf_ca_process_id': ('corporate_action.etf.process_id', pa.int64(), 'identifier', 'dimensionless'),
    'fld_etf_ca_cash_per_share': ('corporate_action.etf.cash_per_share', pa.float64(), 'numeric', 'CNY/share'),
    'fld_etf_ca_split_ratio': ('corporate_action.etf.split_ratio', pa.float64(), 'ratio', 'share/share'),
    'fld_etf_ca_record_date': ('corporate_action.etf.record_date', pa.date32(), 'date', 'day'),
    'fld_etf_ca_ex_date': ('corporate_action.etf.ex_date', pa.date32(), 'date', 'day'),
    'fld_etf_ca_pay_date': ('corporate_action.etf.pay_date', pa.date32(), 'date', 'day'),
    'fld_etf_ca_status': ('corporate_action.etf.status', pa.int8(), 'category', 'dimensionless'),
    'fld_etf_ca_add_time': ('corporate_action.etf.source_added_at', pa.timestamp('us'), 'datetime', 'microsecond'),
    'fld_etf_ca_mod_time': ('corporate_action.etf.source_revised_at', pa.timestamp('us'), 'datetime', 'microsecond'),
}


def _bindings(dataset, table, index):
    names = dict(zip(table.column_names,
        ('date', 'code', 'open', 'high', 'low', 'close', 'volume', 'factor') if index == BARS_REQUEST
        else ('date', 'code', 'open') if index == OPEN_REQUEST else table.column_names))
    inventory = PhysicalInventory('parquet', 'public_synthetic', 'example', 'portfolio_'+index,
        tuple(PhysicalColumn(names[field.name], str(field.type), field.nullable, number)
              for number, field in enumerate(table.schema)))
    original = PhysicalBindingContract('synthetic.'+index+'.original', 1, dataset.dataset_id, 1,
        'synthetic_generator', 'example', 'portfolio_'+index, inventory.schema_revision,
        'drift.strict.v1', {name: {'kind': 'direct', 'column': names[name]} for name in table.column_names})
    archived_inventory = PhysicalInventory('parquet', 'public_synthetic', 'example', 'portfolio_'+index,
        tuple(PhysicalColumn(field.name, str(field.type), field.nullable, number)
              for number, field in enumerate(table.schema)))
    archived = replace(original, binding_id='synthetic.'+index+'.archived', source_profile='public_synthetic',
        expected_schema_revision=archived_inventory.schema_revision,
        column_bindings={name: {'kind': 'direct', 'column': name} for name in table.column_names})
    return original, archived


def portfolio_sources(rows):
    """无行动是生成器定义的合成事实，不为自有数据补默认值。"""
    policies = (
        PolicyContract('available.portfolio.open.v1', 1, 'availability',
                       {'available_after': 'session_open', 'available_time_field': OPEN_FIELDS[0],
                        'timezone': 'Asia/Shanghai'}),
        PolicyContract('available.etf.corporate_action.v1', 3, 'availability',
                       {'available_after': 'next_session_open_after_publication_date',
                        'available_time_field': 'fld_etf_ca_pub_date', 'source_added_time_field': 'fld_etf_ca_add_time',
                        'date_granularity': 'day', 'same_day_use': 'reject', 'timezone': 'Asia/Shanghai'}),
        PolicyContract('revision.etf.corporate_action.v1', 1, 'revision',
                       {'selection': 'latest_revision_available_at_or_before_as_of',
                        'revision_field': 'fld_etf_ca_mod_time', 'future_revision': 'reject'}),
    )
    days = sorted({row['fld_equity_daily_date'] for row in rows})
    # 最终留出价格不进入组合执行来源；初始日只供前日因子与参考价使用。
    end = days[-1]
    source_rows = [row for row in rows if row['fld_equity_daily_date'] <= end]
    bars = pa.Table.from_pylist([dict(zip(BARS_FIELDS, (
        row['fld_equity_daily_date'], row['fld_equity_daily_code'], row['fld_demo_open'],
        row['fld_demo_high'], row['fld_demo_low'], row['fld_equity_daily_close'],
        row['fld_demo_volume'], 1.))) for row in source_rows])
    opening = pa.Table.from_pylist([dict(zip(OPEN_FIELDS, (
        row['fld_equity_daily_date'], row['fld_equity_daily_code'], row['fld_demo_open'])))
        for row in source_rows if row['fld_equity_daily_date'] > days[0]])
    action_clock_fields = ('fld_etf_ca_id', 'fld_etf_ca_code', 'fld_etf_ca_pub_date',
                           'fld_etf_ca_ex_date', 'fld_etf_ca_add_time', 'fld_etf_ca_mod_time')
    actions = pa.Table.from_pylist([], schema=pa.schema([(name, ACTION_FIELDS[name][1]) for name in action_clock_fields]))
    daily_roles = dict(zip(BARS_FIELDS, ('market.etf.daily.date', 'market.etf.daily.code', 'market.etf.daily.open',
        'market.etf.daily.high', 'market.etf.daily.low', 'market.etf.daily.close',
        'market.etf.daily.volume', 'market.etf.daily.factor')))
    open_roles = dict(zip(OPEN_FIELDS, ('market.qlib.open_state.date', 'market.qlib.open_state.code', 'market.qlib.open_state.open')))
    specifications = (
        (BARS_REQUEST, 'public.synthetic.etf.execution_daily', bars, 'available.daily.v1', 'daily', BARS_FIELDS[:2], BARS_FIELDS[0], daily_roles),
        (OPEN_REQUEST, 'cn_equity.session_open_state', opening, 'available.portfolio.open.v1', 'daily', OPEN_FIELDS[:2], OPEN_FIELDS[0], open_roles),
        (ACTION_REQUEST, 'cn_etf.corporate_action', actions, 'available.etf.corporate_action.v1', 'event', ('fld_etf_ca_id',), 'fld_etf_ca_ex_date', {}),
    )
    contracts, sources = list(policies), []
    for request_id, dataset_id, table, availability, frequency, keys, event_time, roles in specifications:
        fields = []
        for field in table.schema:
            name = field.name
            if request_id == ACTION_REQUEST:
                logical, _, semantic, unit = ACTION_FIELDS[name]
            else:
                logical = roles[name]
                semantic = 'date' if pa.types.is_date32(field.type) else 'identifier' if pa.types.is_string(field.type) else 'numeric'
                unit = 'day' if semantic == 'date' else 'dimensionless' if semantic == 'identifier' or name == 'fld_etf_factor' else 'share' if name == 'fld_etf_volume' else 'CNY'
            fields.append(FieldContract(name, logical, 1,
                'timestamp' if pa.types.is_timestamp(field.type) else 'date32' if pa.types.is_date32(field.type)
                else 'float64' if pa.types.is_float64(field.type) else str(field.type), semantic, unit,
                request_id == ACTION_REQUEST, availability, (frequency,), ('etf',),
                adjustment_allowed=('unadjusted',),
                observation_model='point_in_time' if request_id == ACTION_REQUEST else 'market_event' if request_id == OPEN_REQUEST else 'static_reference',
                observation_keys={'available_time_field': 'fld_etf_ca_pub_date', 'revision_field': 'fld_etf_ca_mod_time'}
                    if request_id == ACTION_REQUEST else {'event_time_field': OPEN_FIELDS[0], 'available_time_field': OPEN_FIELDS[0]}
                    if request_id == OPEN_REQUEST else {}))
        dataset = DatasetContract(dataset_id, 1, 'cn_etf', 'etf', frequency, keys, tuple(table.column_names),
            event_time, availability, 'drift.strict.v1', keys,
            revision_policy_id='revision.etf.corporate_action.v1' if request_id == ACTION_REQUEST else 'revision.none.v1',
            result_cardinality='zero_or_more' if request_id == ACTION_REQUEST else 'one_or_more')
        original, archived = _bindings(dataset, table, request_id)
        contracts.extend((*fields, dataset, original, archived))
        if request_id == BARS_REQUEST:
            sources.append((FACTOR_REQUEST, dataset, table, original, archived))
            import pyarrow.compute as pc
            table = table.filter(pc.greater(table[BARS_FIELDS[0]], pa.scalar(days[0])))
        sources.append((request_id, dataset, table, original, archived))
    return tuple(contracts), tuple(sources)
