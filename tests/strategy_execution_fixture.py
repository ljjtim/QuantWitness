"""策略协议声明、证券和分钟数据的共享夹具。"""


from research_pipeline.domain import InstrumentKey
from research_pipeline.domain.strategy_protocol import StrategyDeclaration, StrategyLimits, StrategyInputBinding


A = InstrumentKey("000001.XSHE", "cn_stock", "XSHE", "CNY", "stock")
B = InstrumentKey("600000.XSHG", "cn_stock", "XSHG", "CNY", "stock")


def declaration(**changes):
    return StrategyDeclaration(**{**dict(strategy_id="test", bundle_id="test", state_schema_id="test-state-v1",
        subscriptions=("initialize", "before_trading", "execution_feedback", "bar", "after_trading"), schedules=(),
        input_bindings=(StrategyInputBinding(binding_id="quotes", columns=("close", "volume"), lookback_bars=2),),
        limits=StrategyLimits(max_state_bytes=4096, max_context_bytes=65536, max_response_bytes=65536,
                              max_intents_per_event=20, callback_wall_seconds=3)), **changes})


