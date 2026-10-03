"""日频执行接线的费用、时点和独立金融复核。"""
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from research_pipeline.domain import InstrumentKey, PortfolioTarget
from research_pipeline.domain.trading import PortfolioTargetEntry
from research_pipeline.platform import canonical_json
from research_pipeline.runtime.qlib_portfolio_execution import execute_daily_cash_artifact
from research_pipeline.evidence.financial_oracle.daily_etf import verify_daily_etf_financial_context

ZONE = ZoneInfo('Asia/Shanghai')


def _case(root):
    code = '510300.XSHG'
    instrument = InstrumentKey(code, 'cn_etf', 'XSHG', 'CNY', 'etf')
    targets, benchmarks, market = [], [], []
    for day, weight in ((2, 0.5), (3, 0.0)):
        decision = datetime(2024, 1, day - 1, 15, tzinfo=ZONE)
        target = PortfolioTarget(decision_time=decision, target_type='weight',
            entries=(PortfolioTargetEntry(instrument, 'weight', weight),) if weight else (),
            base_currency='CNY', cash_weight=1-weight, short_allowed=False,
            leverage_limit=1.0, source_hashes=('d'*64,))
        targets.append({'decision_time': decision, 'order_time': datetime(2024,1,day,9,30,tzinfo=ZONE),
            'target_json': canonical_json(target.to_dict()), 'target_hash': target.target_hash,
            'intent_plan_hash': 'e'*64})
        benchmarks.append({'code': code, 'decision_time': decision, 'price_units': 10000, 'price_scale': 3, 'available_at': decision})
        market.append({'date': date(2024,1,day), 'code': code, 'open': 10., 'close': 10.,
            'high_limit':11., 'low_limit':9., 'paused':False})
    for prefix, rows in [('targets',targets),('benchmarks',benchmarks),('market',market)]:
        folder=root/prefix;folder.mkdir(parents=True)
        pq.write_table(pa.Table.from_pylist(rows),folder/'data.parquet')
    return {'market_rule_profile_id':'cn_etf.daily.curated.v1','instrument_codes':[code],
        'bond_etf_codes':[],'equity_etf_codes':[code],'commission_ppm':300,'min_commission_units':5,
        'corporate_actions':[],'initial_cash_cny':2100.,'calendar_id':'cn.exchange.calendar.v1',
        'policy_available_at':'2024-01-01T00:00:00+08:00'}


def _run(root, params):
    return execute_daily_cash_artifact(target_root=root,market_root=root,parameters=params,
        output_root=root/'output',max_memory_bytes=64*1024**2,market_artifact_hash='a'*64,target_artifact_hash='b'*64)


def test_cash_artifact_costs_and_independent_oracle(tmp_path,monkeypatch):
    import duckdb, sqlite3
    def reject(*args,**kwargs):
        pytest.fail('日频接线测试不访问数据库')
    monkeypatch.setattr(duckdb,'connect',reject)
    monkeypatch.setattr(sqlite3,'connect',reject)
    result=_run(tmp_path,_case(tmp_path))
    root=tmp_path/'output/simulation'
    canonical={name:pq.read_table(root/'result-contract'/name).to_pylist()
        for name in ('orders','fills','positions','cash','costs','valuations')}
    assert [row['quantity'] for row in canonical['fills']]==[100,100]
    assert sum(row['fee_units'] for row in canonical['fills'])==60
    metrics={row['metric_ref']:row['value'] for row in pq.read_table(root/'metrics').to_pylist()}
    assert metrics['portfolio.transaction_cost@1.0.0']==pytest.approx(0.6)
    assert metrics['portfolio.total_return@1.0.0']==pytest.approx(-0.6/2100)
    context=json.loads((root/'daily-context.json').read_text(encoding='utf-8'))
    manifest=json.loads((root/'result-contract/manifest.json').read_text(encoding='utf-8'))
    assert manifest['semantics']['settlement_policy_id'] == 'cn.etf.mixed-t0-t1.v1'
    verify_daily_etf_financial_context(context=context,canonical=canonical,simulation_manifest=manifest,
        oracle_input={'source_ledger_hash':result['source_ledger_hash'],
                      'policy':{'asset_class':'cn_etf','bar_frequency':'daily','rule_snapshot_hash':context['rule_bundle_hash']}})


def test_future_benchmark_is_rejected(tmp_path):
    params=_case(tmp_path)
    path=tmp_path/'benchmarks/data.parquet'
    rows=pq.read_table(path).to_pylist()
    rows[0]['available_at']+=timedelta(days=1)
    pq.write_table(pa.Table.from_pylist(rows),path)
    with pytest.raises(ValueError,match='尚不可见'):
        _run(tmp_path,params)


def test_target_reference_mismatch_is_rejected(tmp_path):
    params=_case(tmp_path)
    path=tmp_path/'targets/data.parquet'
    rows=pq.read_table(path).to_pylist();rows[0]['target_hash']='f'*64
    pq.write_table(pa.Table.from_pylist(rows),path)
    with pytest.raises(ValueError,match='身份'):
        _run(tmp_path,params)


@pytest.mark.parametrize("same_day", [True, False])
def test_target_must_execute_on_next_market_session(tmp_path, same_day):
    params = _case(tmp_path)
    path = tmp_path / "targets/data.parquet"
    rows = pq.read_table(path).to_pylist()
    if same_day:
        decision = datetime(2024, 1, 2, 8, tzinfo=ZONE)
        payload = json.loads(rows[0]["target_json"])
        payload["decision_time"] = decision.isoformat()
        target = PortfolioTarget.from_dict(payload)
        rows[0].update(decision_time=decision, target_json=canonical_json(target.to_dict()), target_hash=target.target_hash)
    else:
        rows[0]["order_time"] += timedelta(days=1)
    pq.write_table(pa.Table.from_pylist(rows), path)
    with pytest.raises(ValueError, match="下一交易日"):
        _run(tmp_path, params)


def test_core_builds_target_identity_for_project_json(tmp_path):
    params = _case(tmp_path)
    path = tmp_path / "targets/data.parquet"
    table = pq.read_table(path).drop(["target_hash"])
    pq.write_table(table, path)
    result = _run(tmp_path, params)
    fills = pq.read_table(tmp_path / "output/simulation/result-contract/fills").to_pylist()
    assert [row["quantity"] for row in fills] == [100, 100]
    assert result["claim_level"] == "research_observation"


@pytest.mark.parametrize("scale", [None, 2, 3.5])
def test_daily_cash_benchmark_requires_explicit_mill_scale(tmp_path, scale):
    params = _case(tmp_path)
    path = tmp_path / 'benchmarks/data.parquet'
    rows = pq.read_table(path).to_pylist()
    for row in rows:
        if scale is None:
            row.pop('price_scale')
        else:
            row['price_scale'] = scale
    pq.write_table(pa.Table.from_pylist(rows), path)
    with pytest.raises(ValueError, match='scale'):
        _run(tmp_path, params)
