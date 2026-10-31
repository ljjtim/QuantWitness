"""显式订单与时点的纯测试夹具。"""
from datetime import date, datetime
from zoneinfo import ZoneInfo
from research_pipeline.domain import InstrumentKey, Price
from research_pipeline.domain.order_stream import ExplicitOrderCommand
ZONE = ZoneInfo('Asia/Shanghai')

def _at(hour, minute=0, day=2):
    return datetime(2024, 1, day, hour, minute, tzinfo=ZONE)

def _command(order_id='order-1', *, market='cn_etf', quantity=100, side='buy', order_type='market', tif='DAY', limit=10000, sequence=0, at=None):
    code = '600000.XSHG' if market == 'cn_stock' else '510300.XSHG'
    return ExplicitOrderCommand(command_id=f'submit:{order_id}', action='submit', order_id=order_id, instrument=InstrumentKey(code, market, 'XSHG', 'CNY', 'stock' if market == 'cn_stock' else 'etf'), decision_time=min(_at(9, 20), at or _at(9, 29)), submitted_at=at or _at(9, 29), available_at=min(_at(9, 20), at or _at(9, 29)), source_sequence=sequence, source_hashes=('a' * 64,), trading_date=date(2024, 1, 2), side=side, quantity=quantity, position_effect='auto', order_type=order_type, time_in_force=tif, limit_price=Price(limit, 3, 'CNY') if order_type == 'limit' else None, reference_price=Price(10000, 3, 'CNY'), reference_price_available_at=min(_at(9, 20), at or _at(9, 29)), funds_policy='reject')


