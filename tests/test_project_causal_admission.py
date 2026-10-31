"""项目因果准入须绑定真实图源和 Catalog 时间角色；不访问数据库。"""

from copy import deepcopy
from dataclasses import replace
from datetime import date, datetime

import pytest

from research_pipeline.application.research_plan_store import _request_ids_from_parameters
from research_pipeline.data_plane.admission import AdmittedQueryPlan, MINUTE_AVAILABILITY_RULE
from research_pipeline.data_plane.query_ir import (
    QueryBudget, QueryIR, QueryPurpose, SortKey, DateRangeV1, UniverseSelection,
)
from research_pipeline.data_plane.temporal import (
    SelectionClock, TemporalSelectionPlan, VisibilityFilter,
)
from research_pipeline.packages.models import ResearchPackageError
from research_pipeline.packages.project_causal_admission import (
    validate_project_causal_admitted_sources, validate_project_causal_recipe,
)
from research_pipeline.platform.operator_contracts import (
    InputBinding, OperatorGraphRecipe, OperatorNodeRecipe, OperatorSpec, PortSpec,
)


def _declaration(kind="feature"):
    return {
        "kind": kind, "output_port": "values",
        "key_columns": ["event_id"] if kind == "feature" else ["event_id", "relative_session"],
        "state_scope": "independent",
        "sources": [{
            "port": "data", "request_id": "prices", "columns": ["event", "available", "value"],
            "observation_column": "event", "available_column": "available",
        }],
        "work_items": [{
            "key_rows": [["A"]] if kind == "feature" else [["A", 1]], "decision_time": "2024-01-03T10:00:00+08:00",
            "window_start": "2024-01-02T09:00:00+08:00" if kind == "feature" else "2024-01-03T10:00:00+08:00",
            "window_end": "2024-01-03T10:00:00+08:00" if kind == "feature" else "2024-01-04T10:00:00+08:00",
            "source_partitions": {"data": ["2024-01"]},
        }],
    }


def _recipe(declaration, *, source_request="prices", label_ancestor=False):
    nodes = [
        OperatorNodeRecipe("admission", "data.catalog.admission", "1.0.0", (), {"request_ids": [source_request]}, ()),
        OperatorNodeRecipe("data", "data.columnar.materialize", "1.0.0", (
            InputBinding("admission", "admission", "admission"),
        ), {}, ()),
        OperatorNodeRecipe("project", "project.algorithm", "1.0.0", (
            InputBinding("data", "label" if label_ancestor else "data", "data"),
        ), {"causal_plan": declaration}, ()),
    ]
    if label_ancestor:
        nodes.append(OperatorNodeRecipe("label", "label.algorithm", "1.0.0", (
            InputBinding("data", "data", "data"),
        ), {}, ()))
    return OperatorGraphRecipe.build(graph_id="causal", nodes=tuple(nodes))


class _Registry:
    def __init__(self, kind="feature"):
        self.specs = {}
        for name, artifact in (
            ("project.algorithm", "research.feature-set.v1" if kind == "feature" else "research.label.v1"),
            ("data.columnar.materialize", "data.columnar-bundle.v1"),
            ("label.algorithm", "research.label.v1"),
        ):
            self.specs[name] = OperatorSpec.build(
                operator_id=name, operator_version="1.0.0", input_ports=(),
                output_ports=(PortSpec("values" if name == "project.algorithm" else "data", artifact),),
                parameters=(), strategy_roles=(),
                resource_profile={"memory_bytes": 1, "temp_bytes": 1, "cpu_slots": 1, "wall_seconds": 1},
                determinism_mode="deterministic", seed_policy="none", code_hash="0" * 64,
            )

    def require_operator(self, operator_id, operator_version):
        return self.specs[operator_id]


def _query(purpose=QueryPurpose.FEATURE):
    return QueryIR(
        "fixture.prices", 1, ("code", "event", "available", "value"), purpose,
        DateRangeV1(date(2024, 1, 1), date(2024, 1, 31)), UniverseSelection(("A",)), (),
        (SortKey("event"), SortKey("code")), QueryBudget(100, 100_000), as_of="2024-01-31",
    )


def _admitted(query=None):
    query = query or _query()
    types = (("code", "string"), ("event", "timestamp[us]"), ("available", "timestamp[us]"), ("value", "float64"))
    return AdmittedQueryPlan(
        query=query, catalog_hash="0" * 64, binding_id="fixture", binding_version=1,
        attestation_hash="1" * 64, source_profile="fixture", environment="test",
        expected_schema_revision="schema.v1", availability_policy_hash="2" * 64,
        revision_policy_hash="3" * 64, object_name="prices",
        columns=tuple((name, name) for name in query.field_ids), field_types=types,
        field_nullables=tuple((name, False) for name in query.field_ids),
        primary_key=("code", "event"), event_time_field="event", instrument_field="code",
        temporal_selection=TemporalSelectionPlan(
            SelectionClock("query_as_of"), "event", query.field_ids, query.field_ids,
            types, "Asia/Shanghai", VisibilityFilter("available", True),
        ), input_claim_ceiling="tradable_simulation",
    )


def _validate(recipe, query=None, *, kind="feature", clock="2024-02-01T00:00:00+08:00"):
    validate_project_causal_recipe(
        recipe, request_queries={"prices": query or _query()}, admission=_Registry(kind),
        fixed_clock=datetime.fromisoformat(clock),
    )


@pytest.mark.parametrize("kind", ["feature", "label"])
def test_causal_recipe_and_admitted_market_event_are_bound(kind):
    recipe = _recipe(_declaration(kind))
    query = _query(QueryPurpose.FEATURE if kind == "feature" else QueryPurpose.LABEL)
    _validate(recipe, query, kind=kind)
    validate_project_causal_admitted_sources(recipe, {"prices": _admitted(query)})
    assert _request_ids_from_parameters(recipe.nodes[-1].parameters) == {"prices"}


@pytest.mark.parametrize("column", ["observation_column", "available_column"])
def test_causal_admission_rejects_time_role_lie(column):
    declaration = _declaration()
    declaration["sources"][0][column] = "value"
    with pytest.raises(ResearchPackageError, match="时间列角色"):
        validate_project_causal_admitted_sources(_recipe(declaration), {"prices": _admitted()})


def test_causal_recipe_rejects_known_request_from_other_input():
    with pytest.raises(ResearchPackageError, match="实际输入图来源"):
        _validate(_recipe(_declaration(), source_request="other"))


def test_causal_recipe_rejects_label_ancestor_and_label_request():
    with pytest.raises(ResearchPackageError, match="Label 祖先"):
        _validate(_recipe(_declaration(), label_ancestor=True))
    with pytest.raises(ResearchPackageError, match="Label 请求"):
        _validate(_recipe(_declaration()), _query(QueryPurpose.LABEL))


def test_causal_recipe_rejects_audit_source():
    with pytest.raises(ResearchPackageError, match="AUDIT"):
        _validate(_recipe(_declaration()), _query(QueryPurpose.AUDIT))


@pytest.mark.parametrize("change,match", [
    ("column", "公开投影"), ("window", "日期范围"), ("month", "来源月份"),
])
def test_causal_recipe_rejects_unfrozen_input_scope(change, match):
    declaration = _declaration()
    if change == "column":
        declaration["sources"][0]["columns"].append("unselected")
    elif change == "window":
        declaration["work_items"][0]["window_start"] = "2023-12-31T12:00:00+08:00"
    else:
        declaration["work_items"][0]["source_partitions"]["data"] = ["2023-12"]
    with pytest.raises(ResearchPackageError, match=match):
        _validate(_recipe(declaration))


def test_causal_recipe_rejects_label_horizon_after_fixed_clock():
    with pytest.raises(ResearchPackageError, match="固定时钟"):
        _validate(_recipe(_declaration("label")), kind="label", clock="2024-01-03T11:00:00+08:00")


def test_causal_admission_rejects_unbound_revision_and_unproved_availability():
    admitted = _admitted()
    for temporal in (
        replace(admitted.temporal_selection, selection_clock=SelectionClock("consumer_decision_time")),
        replace(admitted.temporal_selection, visibility_filter=None),
    ):
        with pytest.raises(ResearchPackageError):
            validate_project_causal_admitted_sources(_recipe(_declaration()), {
                "prices": replace(admitted, temporal_selection=temporal),
            })


def test_causal_admission_accepts_only_proved_completed_minute_event_availability():
    declaration = _declaration()
    declaration["sources"][0]["available_column"] = "event"
    admitted = _admitted()
    admitted = replace(
        admitted, temporal_selection=replace(admitted.temporal_selection, visibility_filter=None),
        minute_availability_rule=MINUTE_AVAILABILITY_RULE,
    )
    validate_project_causal_admitted_sources(_recipe(declaration), {"prices": admitted})


def test_causal_plan_changes_recipe_identity_and_keeps_date_timezone():
    declaration = _declaration()
    declaration["work_items"][0]["window_start"] = "2024-01-01T00:00:00+08:00"
    _validate(_recipe(declaration))
    changed = deepcopy(declaration)
    changed["state_scope"] = "carry"
    assert _recipe(declaration).recipe_hash != _recipe(changed).recipe_hash
