"""公开合成 ETF 的日历、交易规则及无公司行动证据。"""
from copy import deepcopy
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

from research_pipeline.domain import SessionPolicyBundle, load_session_policy_bundle
from research_pipeline.domain.minute_rule_snapshots import (
    MinuteRuleSnapshotBundle, PROJECT_RULE_BUNDLE_VERSION, load_minute_rule_snapshot_bundle,
)
from research_pipeline.domain.minute_scope_input import write_minute_scope_input
from research_pipeline.domain.session_calendar import PROJECT_SESSION_BUNDLE_VERSION, PROJECT_SESSION_POLICY_VERSION
from research_pipeline.data_plane.minute_adjustment import build_adjustment_factor_snapshot
from research_pipeline.platform import typed_canonical_hash
from research_pipeline.simulation.corporate_actions import corporate_action_snapshot_hash

import synthetic


def _seal(payload, cls):
    payload['bundle_hash'] = typed_canonical_hash({key: value for key, value in payload.items() if key != 'bundle_hash'})
    return cls.from_dict(payload)


def write_synthetic_scope(package, *, days, entities, clock):
    """只描述生成器定义的合成市场，不复用真实证券的规则事实。"""
    start, end = days[1], days[-1]
    visible = days[0] + 'T00:00:00+08:00'
    provenance = typed_canonical_hash({'source': Path(synthetic.__file__).read_text(encoding='utf-8'),
        'calendar': days, 'entities': entities, 'price_scale': 3, 'settlement_days': 1,
        'quantity_step': 100, 'commission_ppm': 300, 'min_commission_cents': 5,
        'corporate_actions': []})
    calendar = load_session_policy_bundle().to_dict()
    prototype = next(row for row in calendar['policies'] if row['instrument']['asset_class'] == 'cn_etf' and row['revision'] == 1)
    policies = []
    for code in entities:
        policy = deepcopy(prototype)
        policy.update(policy_id='session.public.synthetic.etf.v1', contract_version=PROJECT_SESSION_POLICY_VERSION,
            effective_from=days[0], effective_to=days[-1], trading_dates=list(days), available_at=visible)
        policy['instrument'].update(instrument_id=code,
            metadata_hash=typed_canonical_hash({'instrument_id': code, 'asset_class': 'cn_etf', 'source': provenance}))
        policy['evidence_hashes'] = [row for row in policy['evidence_hashes'] if row['source_id'].startswith('minute.capability.')]
        policy['evidence_hashes'].append({'source_id': 'public.synthetic.etf', 'content_hash': provenance})
        policy['evidence_hashes'].sort(key=lambda row: row['source_id'])
        policies.append(policy)
    calendar.update(bundle_id='public.synthetic.etf.calendar', contract_version=PROJECT_SESSION_BUNDLE_VERSION,
                    policies=policies)
    calendar = _seal(calendar, SessionPolicyBundle)
    reference_rules = load_minute_rule_snapshot_bundle().to_dict()
    rules = {key: value for key, value in reference_rules.items() if key in {'capability_binding', 'bundle_hash'}}
    rules.update(bundle_id='public.synthetic.etf.rules', contract_version=PROJECT_RULE_BUNDLE_VERSION,
        instruments=[row['instrument'] for row in policies], sources=[
            dict(source_id='public.synthetic.calendar', source_kind='repository_snapshot',
                locator='synthetic.py#'+calendar.bundle_hash, published_on=days[0], claim_ceiling='research_observation',
                contract_version='minute-rule-source-v1'),
            dict(source_id='public.synthetic.etf', source_kind='repository_evidence', locator='synthetic.py',
                published_on=days[0], claim_ceiling='research_observation', contract_version='minute-rule-source-v1')], rules=[])
    generated = {(row['fld_equity_daily_code'], row['fld_equity_daily_date'].isoformat()): row
                 for row in synthetic.rows(include_factor_fields=True)}

    def add(code, kind, parameters, *, first=start, last=end, at=visible, revision=1):
        rules['rules'].append(dict(rule_id='rule.cn_fund.'+kind+'.v1', revision=revision,
            instrument_id=code, asset_class='cn_etf', status='supported', effective_from=first, effective_to=last,
            available_at=at, source_ids=['public.synthetic.etf'], parameters=parameters,
            unsupported_reason=None, contract_version='minute-rule-snapshot-v1'))

    for code in entities:
        add(code, 'session', {'session_policy_id': 'session.public.synthetic.etf.v1', 'session_policy_revision': 1})
        add(code, 'instrument_lifecycle', {'listed_date': '2000-01-01', 'delisted_date': '2200-01-01',
            'product_class': 'equity_etf', 'is_st': False, 'listing_phase': 'normal',
            'stock_board': 'fund', 'trading_status': 'trading'})
        add(code, 'lot_size', {'buy_lot_units': 100, 'buy_min_quantity': 100, 'buy_quantity_step': 100,
            'sell_min_quantity': 100, 'sell_quantity_step': 100, 'sell_remainder_allowed': True})
        add(code, 'settlement', {'settlement_days': 1})
        add(code, 'suspension', {'paused': False})
        add(code, 'trading_fee', {'commission_ppm': 300, 'min_commission_units': 5,
            'sell_tax_ppm': 0, 'transfer_fee_ppm': 0, 'cost_model_scope': 'research_assumption'})
        snapshot = build_adjustment_factor_snapshot(instrument_id=code, asset_class='cn_etf',
            as_of=datetime.fromisoformat(clock), applicable_start=datetime.fromisoformat(start+'T09:30:00+08:00'),
            applicable_end=datetime.fromisoformat(end+'T15:01:00+08:00'), initial_factor_date=date.fromisoformat(days[0]),
            initial_factor=Decimal(1), source_revision_hash=provenance, corporate_actions=(), effective_times={}, previous_closes={})
        add(code, 'adjustment_factor_snapshot', {'adjustment_snapshot_identity_hash': snapshot.snapshot_identity_hash,
            'corporate_action_snapshot_hash': corporate_action_snapshot_hash(())})
        for index, day in enumerate(days[1:days.index(end)+1], start=1):
            previous = generated[code, days[index-1]]['fld_equity_daily_close']
            def units(value):
                return int((Decimal(str(value))*1000).to_integral_value(rounding=ROUND_HALF_UP))
            add(code, 'price_limit', {'price_limit_mode': 'bounded', 'price_scale': 3,
                'price_limit_ratio_ppm': 100000, 'price_limit_rounding': 'half_up_to_quote_unit',
                'high_limit_units': units(Decimal(str(previous))*Decimal('1.1')),
                'low_limit_units': units(Decimal(str(previous))*Decimal('0.9')),
                'reference_previous_close_units': units(previous), 'reference_price_available_at': day+'T09:30:00+08:00'},
                first=day, last=day, at=day+'T09:30:00+08:00', revision=index)
    rules['rules'].sort(key=lambda row: (row['instrument_id'], row['rule_id'], row['revision']))
    rules = _seal(rules, MinuteRuleSnapshotBundle)
    return write_minute_scope_input(package, name='market', calendar=calendar, rules=rules)
