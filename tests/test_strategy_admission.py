"""项目策略的公共编译、Result闭合及实际项目调度。"""
from dataclasses import replace

import pytest
import yaml

from research_pipeline.domain import InstrumentKey
from research_pipeline.extensions import OperatorSpec, ParameterSpec, ParameterType, PortSpec, compile_project_operator_bundle, project_source_hash, build_admitted_operator_registry
from research_pipeline.runtime.operator_registry import build_mainline_operator_registry
from research_pipeline.packages import compile_research_package, load_research_package, ResearchPackageError
from research_pipeline.results.contracts import STRATEGY_SCHEMA_IDS, STRATEGY_PATHS
from tests.test_minute_project_scope import _project_package, CODE
from tests.strategy_execution_fixture import declaration











@pytest.mark.parametrize("case", ["shared", "different_instrument", "visible_revision", "future_revision"])
def test_schedule_scope_checks_each_instrument_and_historical_day(case):
    from copy import deepcopy
    from types import SimpleNamespace
    from research_pipeline.domain import SessionPolicyBundle
    from research_pipeline.domain.strategy_protocol import StrategySchedule
    from research_pipeline.packages.project_strategy_admission import validate_strategy_schedules
    from tests.test_minute_project_scope import project_inputs, seal, CODE, OTHER, START, END

    calendar, _ = project_inputs(codes=(CODE, OTHER))
    payload = calendar.to_dict()
    if case == "different_instrument":
        payload["policies"][1]["segments"][-1]["end_time"] = "14:30:00"
    elif case in {"visible_revision", "future_revision"}:
        for policy in calendar.policies:
            later = deepcopy(policy.to_dict())
            later.update(revision=2, previous_policy_hash=policy.policy_hash,
                         available_at=("2024-01-09T00:00:00+08:00" if case == "visible_revision"
                                       else "2024-01-10T00:00:00+08:00"))
            later["segments"][-1]["end_time"] = "14:30:00"
            payload["policies"].append(later)
        payload["policies"].sort(key=lambda item: (item["policy_id"], item["revision"], item["instrument"]["instrument_id"]))
    calendar = seal(payload, SessionPolicyBundle)
    declared = declaration(subscriptions=("scheduled",),
                           schedules=(StrategySchedule(schedule_id="late", basis="local_time", at="14:45:00"),))
    query = SimpleNamespace(universe=SimpleNamespace(instruments=(CODE, OTHER)),
                            time_range=SimpleNamespace(start_at=START, end_at=END))
    if case in {"shared", "future_revision"}:
        validate_strategy_schedules(declared.to_dict(), calendar, query)
    else:
        with pytest.raises(ResearchPackageError, match=("共同交易时段" if case == "different_instrument" else "合法分钟触发点")):
            validate_strategy_schedules(declared.to_dict(), calendar, query)
