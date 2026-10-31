"""把已可见组合目标转换为基于真实账户的 LEAN 差额订单。"""
from datetime import datetime
from decimal import Decimal, ROUND_FLOOR


WEIGHT_SCALE = 1_000_000_000


def _amount(value):
    return Decimal(value['units']) / Decimal(10) ** value['scale']


def _values(reference):
    values = reference['values']
    return values if isinstance(values, dict) else dict(values)


def _lot_size(parameters, code, now):
    rows = [row for row in parameters['strategy_rules']
            if row['instrument_id'] == code and row['rule_id'].endswith('.lot_size.v1')
            and row['status'] == 'supported'
            and row['effective_from'] <= now.date().isoformat() <= row['effective_to']
            and datetime.fromisoformat(row['available_at']) <= now]
    if not rows:
        raise ValueError('组合缺少当时可见的数量规则：' + code)
    rule = max(rows, key=lambda row: (datetime.fromisoformat(row['available_at']), row['revision']))
    values = dict(rule['parameters'])
    size = values['buy_lot_units']
    if type(size) is not int or size <= 0:
        raise ValueError('组合数量格点必须为正整数')
    return size


def target_references(target_rows, instruments, *, trading_date):
    """保留原目标时点，按执行会话交付完整权重批；不读取未来目标。"""
    import json

    now = datetime.fromisoformat(str(trading_date)[:10] + 'T09:30:00+08:00')
    eligible = [row for row in target_rows
                if datetime.fromisoformat(str(row['order_time'])) == now]
    if not eligible:
        return []
    if len(eligible) != 1:
        raise ValueError('组合每个执行会话只能有一个完整目标')
    row = eligible[0]
    decision = datetime.fromisoformat(str(row['decision_time']))
    if decision >= now:
        raise ValueError('组合目标必须在下一执行会话前可见')
    target = json.loads(row['target_json'])
    if datetime.fromisoformat(target['decision_time']) != decision or target['target_type'] != 'weight':
        raise ValueError('组合目标时点或类型不一致')
    weights = {}
    for entry in target['entries']:
        code = entry['instrument']['instrument_id']
        weight = Decimal(str(entry['value']))
        if code not in instruments or code in weights or not weight.is_finite() or not 0 <= weight <= 1:
            raise ValueError('目标证券、重复行或权重无效')
        weights[code] = int((weight * WEIGHT_SCALE).to_integral_value(rounding=ROUND_FLOOR))
    if sum(weights.values()) > WEIGHT_SCALE:
        raise ValueError('组合目标不能超过全额权益')
    # 零权重也交付，保证原有但不再入选的持仓进入差额计算。
    return [dict(input_id=f"portfolio:{decision.isoformat()}:{code}", binding_id='portfolio_targets',
                 available_at=now.isoformat(), instrument=instrument,
                 values={'weight_ppb': weights.get(code, 0)})
            for code, instrument in sorted(instruments.items())]


def run(context, event, state):
    """每会话先减仓，成交反馈后按真实可用资金增仓；同目标不重复下单。"""
    state = dict(state)
    declaration = context['strategy']
    response = dict(protocol_version=declaration['protocol_version'], event_seq=event['event_seq'],
                    state_version=declaration['state_version'], state_schema_id=declaration['state_schema_id'],
                    state=state, intents=[])
    if event['kind'] not in {'bar', 'execution_feedback'}:
        return response
    now = datetime.fromisoformat(event['decision_time'])
    if now.time().isoformat() != '09:30:00':
        return response
    references = [row for row in event['inputs'] if row['binding_id'] == 'portfolio_targets']
    if not references:
        return response
    if any(datetime.fromisoformat(row['available_at']) > now for row in references):
        raise ValueError('组合读取了未来目标')
    batch_id = '|'.join(sorted(row['input_id'] for row in references))
    if state.get('completed_target') == batch_id:
        return response
    account = event['account']
    if account['active_orders']:
        return response
    positions = {row['instrument']['instrument_id']: row for row in account['positions']}
    targets = {row['instrument']['instrument_id']: row for row in references}
    if len(targets) != len(references) or set(positions) - set(targets):
        raise ValueError('组合目标批必须完整覆盖证券与实际持仓')
    quotes = {}
    for code in sorted(targets):
        quote = context['market'].quote('prices', code)
        if quote is None:
            raise ValueError('组合缺少当时可见估值报价：' + code)
        if datetime.fromisoformat(quote['available_at']) > now:
            raise ValueError('组合估值报价尚不可见')
        quotes[code] = quote
    cash = _amount(account['available_cash'])
    equity = cash + sum(Decimal(row['quantity']) * _amount(quotes[code]['price'])
                        for code, row in positions.items())
    equity += Decimal(str(account.get('corporate_entitlements', {}).get('additional_equity', 0)))
    if equity < 0:
        raise ValueError('组合权益不能为负')
    if state.get('target_batch') != batch_id:
        state.update(target_batch=batch_id, target_equity=str(equity), sold=False, bought=False)
    base = Decimal(state['target_equity'])
    desired, lots = {}, {}
    for code, row in targets.items():
        weight = _values(row)['weight_ppb']
        if type(weight) is not int or not 0 <= weight <= WEIGHT_SCALE:
            raise ValueError('组合权重必须为十亿分比整数')
        lots[code] = _lot_size(context['parameters'], code, now)
        desired[code] = int(base * weight / WEIGHT_SCALE / _amount(quotes[code]['price']) / lots[code]) * lots[code]

    def submit(code, side, quantity):
        index = len(response['intents'])
        response['intents'].append(dict(intent_id=f"{event['event_seq']}:{index}", kind='quantity',
            instrument=targets[code]['instrument'], side=side, quantity=quantity, amount=None, weight=None,
            order_type='market', time_in_force='DAY', limit_price=None, protection_price=None,
            funds_policy='resize', price_input_id=quotes[code]['input_id'], order_id=None))

    if not state['sold']:
        for code in sorted(targets):
            position = positions.get(code, {'quantity': 0, 'sellable_quantity': 0})
            excess = max(0, int(position['quantity']) - desired[code])
            quantity = min(excess, int(position['sellable_quantity']))
            # 非清仓卖单按历史格点；清仓可交给引擎处理实际零股规则。
            if quantity != int(position['quantity']):
                quantity = quantity // lots[code] * lots[code]
            if quantity:
                submit(code, 'sell', quantity)
        state['sold'] = True
        if response['intents']:
            return response
    if not state['bought']:
        for code in sorted(targets):
            deficit = max(0, desired[code] - int(positions.get(code, {}).get('quantity', 0)))
            if deficit:
                # 金额预算由原生引擎按实际开盘价、费用、现金及数量规则裁剪。
                amount = min(cash, Decimal(deficit) * _amount(quotes[code]['price']))
                units = int((amount * 100).to_integral_value(rounding=ROUND_FLOOR))
                if units <= 0:
                    continue
                submit(code, 'buy', deficit)
                response['intents'][-1].update(kind='amount', quantity=None,
                    amount={'units': units, 'scale': 2, 'currency': 'CNY'})
                cash -= Decimal(units) / 100
        state['bought'] = True
    state['completed_target'] = batch_id
    return response
