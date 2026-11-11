"""在共享账户开立双向仓，下一交易日显式平昨仓。"""


def run(context, event, state):
    intents = []
    if event['kind'] == 'bar':
        day = event['trading_date']
        if not state.get('opened'):
            for number, (code, side) in enumerate((('IF2401.CCFX', 'buy'), ('IF2401.CCFX', 'sell'), ('IF2403.CCFX', 'buy'))):
                intents.append(dict(intent_id=f"{event['event_seq']}:{number}", kind='quantity',
                    instrument={'instrument_id': code}, side=side, position_effect='open', quantity=1,
                    order_type='market', time_in_force='DAY', limit_price=None, protection_price=None))
            state = dict(opened=True, day=day, closed=False)
        elif day != state['day'] and not state['closed']:
            for number, (code, side) in enumerate((('IF2401.CCFX', 'sell'), ('IF2401.CCFX', 'buy'), ('IF2403.CCFX', 'sell'))):
                intents.append(dict(intent_id=f"{event['event_seq']}:{number}", kind='quantity',
                    instrument={'instrument_id': code}, side=side, position_effect='close_yesterday', quantity=1,
                    order_type='market', time_in_force='DAY', limit_price=None, protection_price=None))
            state = dict(state, closed=True)
    return dict(protocol_version='research-strategy-protocol-v4', event_seq=event['event_seq'],
                state_version='research-strategy-json-state-v1', state_schema_id='public.lean.futures.state.v1', state=state, intents=intents)
