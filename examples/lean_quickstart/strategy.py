"""首个可见报价买入100份，下一会话按可卖数量卖出。"""
from datetime import datetime

def run(context, event, state):
    state=dict(state)
    declaration=context["strategy"]
    response=dict(protocol_version=declaration["protocol_version"], event_seq=event["event_seq"],
        state_version=declaration["state_version"], state_schema_id=declaration["state_schema_id"], state=state, intents=[])
    if event["kind"] != "bar" or state.get("sold"):
        return response
    code=context["parameters"]["instruments"][0]["instrument_id"]
    quote=context["market"].quote("prices", code)
    if quote is None or event["account"]["active_orders"]:
        return response
    now=datetime.fromisoformat(event["decision_time"])
    if datetime.fromisoformat(quote["available_at"]) > now:
        raise ValueError("报价尚不可见")
    day=now.date().isoformat()
    position=next((row for row in event["account"]["positions"] if row["instrument"]["instrument_id"]==code),None)
    if not state.get("bought"):
        side="buy"
        state.update(bought=True, buy_session=day)
    elif day > state["buy_session"] and position and position["sellable_quantity"] >= 100:
        side="sell"
        state["sold"]=True
    else:
        return response
    response["intents"]=[dict(intent_id=str(event["event_seq"])+":0", kind="quantity",
        instrument=quote["instrument"], side=side, quantity=100, amount=None, weight=None,
        order_type="market", time_in_force="DAY", limit_price=None, protection_price=None,
        funds_policy="reject", price_input_id=quote["input_id"], order_id=None)]
    return response
