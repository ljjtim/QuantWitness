"""组合桥接的手算案例；只使用内存账户与可见报价。"""
from copy import deepcopy
from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from extension.lean_strategy import run, target_references


def case(*, cash=1000, positions=(), weights=(500_000_000, 0), active_orders=()):
    instruments = [dict(instrument_id=code, asset_class='cn_etf', venue='XSHG', currency='CNY',
                        contract_kind='etf', contract_version='research-instrument-key-v1')
                   for code in ('SYN000.XSHG', 'SYN001.XSHG')]
    quotes = {instrument['instrument_id']: dict(instrument=instrument, input_id='quote:' + str(index),
              price=dict(units=10000, scale=3, currency='CNY'), available_at='2024-01-03T09:30:00+08:00')
              for index, instrument in enumerate(instruments)}
    context = dict(strategy=dict(protocol_version='protocol', state_version='state', state_schema_id='portfolio.v1'),
        parameters={'strategy_rules': [dict(rule_id='rule.cn_fund.lot_size.v1', instrument_id=row['instrument_id'],
            status='supported', effective_from='2024-01-01', effective_to='2024-12-31',
            available_at='2023-12-29T00:00:00+08:00', revision=1, parameters={'buy_lot_units': 10})
            for row in instruments]}, market=SimpleNamespace(quote=lambda binding, code: deepcopy(quotes[code])))
    event = dict(kind='bar', event_seq=1, decision_time='2024-01-03T09:30:00+08:00',
        account=dict(available_cash=dict(units=cash*100, scale=2, currency='CNY'),
            positions=[dict(instrument=instruments[index], quantity=quantity, sellable_quantity=sellable)
                       for index, quantity, sellable in positions], active_orders=list(active_orders)),
        inputs=[dict(input_id='target:'+row['instrument_id'], binding_id='portfolio_targets', instrument=row,
                     available_at='2024-01-03T09:30:00+08:00', values={'weight_ppb': weight})
                for row, weight in zip(instruments, weights)])
    return context, event


def test_initial_cash_uses_amount_budget_and_repeated_target_does_not_trade():
    context, event = case()
    result = run(context, event, {})
    assert [(row['kind'], row['side'], row['amount']['units']) for row in result['intents']] == [('amount', 'buy', 50000)]
    assert run(context, event, result['state'])['intents'] == []


def test_rotation_waits_for_actual_sell_cash_before_buying():
    context, event = case(cash=0, positions=((1, 100, 100),), weights=(1_000_000_000, 0))
    result = run(context, event, {})
    assert [(row['side'], row['quantity']) for row in result['intents']] == [('sell', 100)]
    event['kind'] = 'execution_feedback'
    event['account']['active_orders'] = [{'order_id': '1'}]
    assert run(context, event, result['state'])['intents'] == []
    event['account'].update(active_orders=[], positions=[], available_cash={'units': 100000, 'scale': 2, 'currency': 'CNY'})
    result = run(context, event, result['state'])
    assert [(row['side'], row['amount']['units']) for row in result['intents']] == [('buy', 100000)]


def test_unsellable_position_does_not_fund_a_buy():
    context, event = case(cash=0, positions=((1, 100, 0),), weights=(1_000_000_000, 0))
    assert run(context, event, {})['intents'] == []


def test_quantity_rule_is_selected_at_current_time():
    context, event = case()
    future = deepcopy(context['parameters']['strategy_rules'][0])
    future.update(revision=2, available_at='2024-01-04T00:00:00+08:00', parameters={'buy_lot_units': 100})
    context['parameters']['strategy_rules'].append(future)
    assert run(context, event, {})['intents'][0]['amount']['units'] == 50000


def test_future_quote_is_rejected():
    context, event = case()
    context['market'] = SimpleNamespace(quote=lambda binding, code: dict(price={'units': 10000, 'scale': 3},
        available_at='2024-01-04T09:30:00+08:00', input_id='future'))
    with pytest.raises(ValueError, match='尚不可见'):
        run(context, event, {})


def test_target_batch_is_delivered_only_on_execution_day():
    import json
    instrument = dict(instrument_id='SYN000.XSHG')
    target = dict(decision_time='2024-01-02T09:31:00+08:00', target_type='weight',
                  entries=[{'instrument': instrument, 'value': Decimal('0.3')}])
    row = dict(decision_time=target['decision_time'], order_time='2024-01-03T09:30:00+08:00',
               target_json=json.dumps(target, default=str))
    instruments = {'SYN000.XSHG': instrument}
    assert target_references([row], instruments, trading_date='2024-01-02') == []
    assert target_references([row], instruments, trading_date='2024-01-03')[0]['values']['weight_ppb'] == 300000000
    row['decision_time'] = '2024-01-04T09:31:00+08:00'
    with pytest.raises(ValueError, match='必须在下一执行会话前可见'):
        target_references([row], instruments, trading_date='2024-01-03')


def test_formal_target_port_reaches_daily_selection(tmp_path):
    import json
    from datetime import date
    import pyarrow as pa
    import pyarrow.parquet as pq
    from research_pipeline.domain import InstrumentKey
    from research_pipeline.domain.strategy_protocol import StrategyInputBinding, StrategyLimits
    from research_pipeline.runtime.external_artifact import ExternalArtifactStore
    from research_pipeline.runtime.operator_runtime import RuntimeNodeValue
    from research_pipeline.runtime.lean_strategy_inputs import _signals

    instruments = (InstrumentKey('SYN000.XSHG', 'cn_etf', 'XSHG', 'CNY', 'etf'),
                   InstrumentKey('SYN001.XSHG', 'cn_etf', 'XSHG', 'CNY', 'etf'))
    decision = datetime.fromisoformat('2024-01-02T09:31:00+08:00')
    execution = datetime.fromisoformat('2024-01-03T09:30:00+08:00')
    target = dict(decision_time=decision.isoformat(), target_type='weight',
        entries=[dict(instrument=instruments[0].to_dict(), target_type='weight', value=0.3)],
        base_currency='CNY', cash_weight=0.7, short_allowed=False, leverage_limit=1.,
        source_hashes=['a'*64], contract_version='research-portfolio-target-v1')
    store = ExternalArtifactStore(tmp_path/'store')
    staging = store.prepare()
    path = staging/'targets/part-00000.parquet'
    path.parent.mkdir()
    pq.write_table(pa.Table.from_pylist([dict(decision_time=decision, order_time=execution,
        target_json=json.dumps(target))]), path)
    commit = store.commit(staging, artifact_name='targets', artifact_type='research.portfolio-targets.v1')
    context = SimpleNamespace(inputs={'targets': RuntimeNodeValue.external(commit)}, external_store=store,
        effective_resource_budget=SimpleNamespace(memory_bytes=256*1024**2))
    declaration = SimpleNamespace(input_bindings=(StrategyInputBinding(binding_id='portfolio_targets',
        source='signals', columns=('weight_ppb',), lookback_bars=1),),
        limits=StrategyLimits(max_state_bytes=65536, max_context_bytes=65536, max_response_bytes=65536,
            max_intents_per_event=10, callback_wall_seconds=10))
    assert _signals(context, declaration, instruments, '2024-01-02')['selection'] == []
    rows = _signals(context, declaration, instruments, '2024-01-03')['selection']
    assert len(rows) == 2
    assert [dict(row['values'])['weight_ppb'] for row in rows] == [300000000, 0]
    assert all(row['available_at'] == execution.isoformat() for row in rows)


def test_synthetic_execution_sources_exclude_holdout_and_separate_open_clock():
    from lean_inputs import portfolio_sources, BARS_REQUEST, OPEN_REQUEST, FACTOR_REQUEST, ACTION_REQUEST
    import synthetic

    contracts, sources = portfolio_sources(synthetic.rows(include_factor_fields=True))
    sources = {item[0]: item for item in sources}
    bars = sources[BARS_REQUEST][2]
    opening = sources[OPEN_REQUEST][2]
    factors = sources[FACTOR_REQUEST][2]
    cutoff = synthetic.sessions()[-24]
    assert max(bars['fld_etf_date'].to_pylist()) == cutoff
    assert set(bars['fld_etf_date'].to_pylist()) == set(opening['fld_etf_open_state_date'].to_pylist())
    assert min(factors['fld_etf_date'].to_pylist()) < min(bars['fld_etf_date'].to_pylist())
    assert sources[ACTION_REQUEST][2].num_rows == 0
    policy = next(item for item in contracts if getattr(item, 'policy_id', None) == 'available.portfolio.open.v1')
    assert policy.rules['available_after'] == 'session_open'
    field = next(item for item in contracts if getattr(item, 'field_id', None) == 'fld_etf_open_state_open')
    assert field.observation_keys['available_time_field'] == 'fld_etf_open_state_date'
