"""组合执行所需的显式 LEAN 安装与历史来源配置。"""
from copy import deepcopy
from decimal import Decimal
from pathlib import Path
import shutil

from research_pipeline.domain.minute_scope_input import load_minute_scope_input
from research_pipeline.platform.lean_runtime import freeze_lean_runtime

from lean_inputs import ACTION_REQUEST, BARS_REQUEST, FACTOR_REQUEST, OPEN_REQUEST


EXECUTION_FIELDS = {
    'scope_root', 'minute_scope', 'bars_request_id', 'open_request_id', 'factor_request_id',
    'corporate_action_request_id', 'applicable_start', 'applicable_end', 'availability_policy_ref',
    'slippage_bps', 'slippage_ticks', 'participation_ppm',
}


def prepare_execution(root, design, *, own=None, lean_home=None):
    """真实来源必须完整声明；合成来源只生成明确的合成规则。"""
    root = Path(root)
    package = root/'package'
    if own is None:
        from lean_scope import write_synthetic_scope
        reference = write_synthetic_scope(package, days=design['calendar_sessions'],
            entities=design['entities'], clock=design['fixed_clock'])
        days = design['calendar_sessions']
        execution = dict(minute_scope=reference, bars_request_id=BARS_REQUEST, open_request_id=OPEN_REQUEST,
            factor_request_id=FACTOR_REQUEST, corporate_action_request_id=ACTION_REQUEST,
            applicable_start=days[1]+'T09:30:00+08:00', applicable_end=days[-24]+'T15:01:00+08:00',
            availability_policy_ref='available.etf.corporate_action.v1', slippage_bps=0, slippage_ticks=0,
            participation_ppm=1_000_000)
    else:
        execution = deepcopy(own['lean_execution'])
        if not EXECUTION_FIELDS <= set(execution) or set(execution)-EXECUTION_FIELDS-{'lean_home'}:
            raise ValueError('自有组合必须完整声明LEAN来源、scope、窗口、滑点及参与率')
        lean_home = lean_home or execution.pop('lean_home', None)
        scope = load_minute_scope_input(execution['scope_root'], execution['minute_scope'])
        target = package/'minute-scopes'/execution['minute_scope']['name']
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(scope.root/'minute-scopes'/execution['minute_scope']['name'], target)
        execution.pop('scope_root')
    if not lean_home:
        raise ValueError('portfolio模式必须显式提供--lean-home或lean_execution.lean_home')
    execution['lean_runtime'] = freeze_lean_runtime(lean_home)
    cash = Decimal(str(design['finance']['initial_cash_cny']))*100
    if not cash.is_finite() or cash <= 0 or cash != cash.to_integral_value():
        raise ValueError('组合初始现金必须为正且精确到分')
    execution.update(initial_cash_units=int(cash), as_of=design['fixed_clock'])
    return execution
