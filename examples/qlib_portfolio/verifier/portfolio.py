"""独立核对固定test组合的预测来源、目标、决策基准与执行日。"""
from collections import defaultdict
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal
import json
import math


ZONE = timezone(timedelta(hours=8))


def require(condition, message):
    if not condition:
        raise ValueError(message)


def moment(value):
    result = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    require(result.tzinfo is not None, '组合时点缺少时区')
    return result


def verify_portfolio(tables, design):
    sessions = design['calendar_sessions']
    predictions = defaultdict(dict)
    for row in tables['test_predictions']:
        day = str(row['observation_session'])[:10]
        code = row['entity_id']
        require(code not in predictions[day], '同会话预测证券重复')
        predictions[day][code] = row
    require(len(tables['portfolio_targets']) == len(predictions), '目标与test会话数不同')
    targets = {str(row['decision_time'].date()): row for row in tables['portfolio_targets']}
    require(targets.keys() == predictions.keys(), '组合目标没有逐会话绑定test预测')
    benchmarks = {(str(row['decision_time'].date()), row['code']): row for row in tables['decision_benchmarks']}
    require(len(benchmarks) == len(tables['decision_benchmarks']), '决策基准重复')
    require(set(benchmarks) == {(day, code) for day in predictions for code in design['entities']}, '决策基准没有覆盖冻结证券池')
    prices = {(str(row['session'])[:10], row['entity_id']): row['close'] for row in tables['raw_prices']}
    order_days = []
    for day, rows in sorted(predictions.items()):
        require(set(rows) == set(design['entities']), '固定证券池预测不完整')
        raw = targets[day]
        target = json.loads(raw['target_json'])
        decision = moment(raw['decision_time'])
        execution = moment(raw['order_time'])
        require(decision == datetime.combine(decision.date(), time(9, 31), ZONE), '组合决策时间不是当日09:31')
        require(decision < moment(design['holdout_start']), '最终holdout进入交易目标')
        require(execution.isoformat() == sessions[sessions.index(day)+1] + 'T09:30:00+08:00', '执行不是下一会话开盘')
        require(moment(target['decision_time']) == decision, '目标内部决策时点不一致')
        ordered = sorted(rows, key=lambda code: (-rows[code]['prediction'], code))[:3]
        entries = {item['instrument']['instrument_id']: item for item in target['entries']}
        require(len(target['entries']) == 3 and set(entries) == set(ordered), '目标不是固定最高三只')
        require(target['target_type'] == 'weight' and target['short_allowed'] is False and target['leverage_limit'] == 1., '目标杠杆或类型不同')
        require(math.isclose(target['cash_weight'], .1, abs_tol=1e-12), '现金权重不同')
        for entry in entries.values():
            require(entry['target_type'] == 'weight' and math.isclose(entry['value'], .3, abs_tol=1e-12), '目标等权规则不同')
        for code, prediction in rows.items():
            require(prediction['stage'] == 'test' and math.isfinite(prediction['prediction']), '目标消费了非test预测')
            require(moment(prediction['feature_available_time']) <= moment(prediction['decision_time']) <= decision, '目标使用了不可见预测')
            benchmark = benchmarks[(day, code)]
            prior_close = prices[(sessions[sessions.index(day)-1], code)]
            units = Decimal(str(prior_close)) * 1000
            require(units == units.to_integral_value(), '前会话收盘价格超过三位小数')
            require(benchmark['price_scale'] == 3 and benchmark['price_units'] == int(units), '决策基准不是前会话收盘')
            require(moment(benchmark['available_at']) == datetime.combine(decision.date(), time(9, 30), ZONE), '基准不符合次会话开盘可见策略')
        order_days.append(execution.date())
    require(order_days, '组合没有目标')
    execution_market = {(row['date'], row['code']): row for row in tables['execution_market']}
    expected_days = [datetime.fromisoformat(day).date() for day in sessions if min(order_days).isoformat() <= day <= max(order_days).isoformat()]
    require(set(execution_market) == {(day, code) for day in expected_days for code in design['entities']}, '执行行情缺少会话或证券')
    for (day, code), row in execution_market.items():
        require(row['close'] == prices[(day.isoformat(), code)], '执行收盘估值与封存原价不同')
    orders = tables['research.simulation.orders']
    for order in orders:
        day = moment(order['decision_time']).date().isoformat()
        require(day in targets and moment(order['decision_time']) == moment(targets[day]['decision_time']), '规范订单无法绑定正式目标时点')

    valuations = sorted(tables['research.simulation.valuations'], key=lambda row: moment(row['valuation_time']))
    nav = [row['nav_units'] / 100 for row in valuations]
    require(nav and len(nav) == len(expected_days), '组合净值会话不完整')
    metrics = {row['metric_ref']: row for row in tables['research.daily-simulation.metrics']}
    peak = nav[0]
    drawdown = 0.
    for value in nav:
        peak = max(peak, value)
        drawdown = min(drawdown, value / peak - 1.)
    fills = tables['research.simulation.fills']
    initial_cash = float(design.get('finance', {}).get('initial_cash_cny', 100000.))
    expected_metrics = {
        'portfolio.total_return@1.0.0': nav[-1] / initial_cash - 1.,
        'portfolio.max_drawdown@1.0.0': drawdown,
        'portfolio.turnover@1.0.0': sum(row['notional_units'] for row in fills) / 100 / initial_cash,
        'portfolio.transaction_cost@1.0.0': sum(row['fee_units'] for row in fills) / 100,
    }
    require(set(metrics) == set(expected_metrics), '组合绩效指标集合不同')
    for ref, value in expected_metrics.items():
        require(math.isclose(metrics[ref]['value'], value, rel_tol=1e-10, abs_tol=1e-12), '组合指标无法从规范净值或成交复算：' + ref)
