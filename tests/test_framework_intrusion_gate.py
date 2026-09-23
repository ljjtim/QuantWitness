from __future__ import annotations

import json
from pathlib import Path

import pytest

from research_pipeline.operations.framework_boundary import audit_framework_boundary


def test_public_core_matches_framework_boundary_policy() -> None:
    project = Path(__file__).resolve().parents[1]
    result = audit_framework_boundary(
        project,
        source_root=project / "src/research_pipeline",
        policy_path=project / "release/framework-boundary/policy.json",
    )
    assert result["status"] == "pass", result["issues"]


def _policy(path, *, token="project_only", expires_at="2099-12-31"):
    path.write_text(json.dumps({
        "contract_version": "research-framework-boundary-v1",
        "forbidden_import_prefixes": ["research_pipeline.project_extensions"],
        "forbidden_tokens": [token],
        "scan_paths": ["."],
        "exemptions": [],
    }), encoding="utf-8")


def test_forbidden_import_and_token_fail(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "bad.py").write_text("import research_pipeline.project_extensions.x\n# project_only\n", encoding="utf-8")
    policy = tmp_path / "policy.json"
    _policy(policy)
    result = audit_framework_boundary(tmp_path, source_root=source, policy_path=policy)
    assert result["status"] == "fail"
    assert {item["code"] for item in result["issues"]} == {"forbidden_import", "forbidden_token"}


def test_core_cannot_import_top_level_project_verifier(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "bad.py").write_text(
        "from project_extensions.study.verifier import verify\n",
        encoding="utf-8",
    )
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({
        "contract_version": "research-framework-boundary-v2",
        "forbidden_import_prefixes": ["project_extensions"],
        "scan_paths": ["."],
    }), encoding="utf-8")

    observed = audit_framework_boundary(
        tmp_path, source_root=source, policy_path=policy,
    )
    assert {item["code"] for item in observed["issues"]} == {"forbidden_import"}


@pytest.mark.parametrize(
    "token",
    ("derived_causal_plan", "next_calendar_month_last_session"),
)
def test_monthly_project_causal_contract_cannot_return_to_core(
    tmp_path: Path,
    token: str,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "bad.py").write_text(
        f"PROJECT_POLICY = {token!r}\n",
        encoding="utf-8",
    )
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({
        "contract_version": "research-framework-boundary-v2",
        "forbidden_tokens": [token],
        "scan_paths": ["."],
    }), encoding="utf-8")

    observed = audit_framework_boundary(
        tmp_path, source_root=source, policy_path=policy,
    )
    assert {item["code"] for item in observed["issues"]} == {"forbidden_token"}


@pytest.mark.parametrize(
    "source_text",
    (
        "def require_futures_d0_receipt(payload, *, stage):\n    return payload\n",
        'require_futures_d0_receipt(report, stage="model")\n',
    ),
)
def test_generic_capability_cannot_route_by_project_stage(
    tmp_path: Path,
    source_text: str,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "bad.py").write_text(source_text, encoding="utf-8")
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({
        "contract_version": "research-framework-boundary-v2",
        "scan_paths": ["."],
        "structural_rules": {
            "project_stage_agnostic_calls": ["require_futures_d0_receipt"],
        },
    }), encoding="utf-8")

    observed = audit_framework_boundary(
        tmp_path, source_root=source, policy_path=policy,
    )
    assert {item["code"] for item in observed["issues"]} == {
        "project_stage_routed_capability",
    }


def test_verification_result_cannot_interpret_unregistered_schema_or_path(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    target = source / "research_pipeline/evidence/verification_result.py"
    target.parent.mkdir(parents=True)
    target.write_text(
        'def verify(snapshot):\n'
        '    schema_id = "research.generic-project-output.v1"\n'
        '    if schema_id in snapshot.schemas:\n'
        '        return snapshot.read("project/output")\n',
        encoding="utf-8",
    )
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({
        "contract_version": "research-framework-boundary-v2",
        "scan_paths": ["."],
    }), encoding="utf-8")

    observed = audit_framework_boundary(
        tmp_path, source_root=source, policy_path=policy,
    )
    assert {item["code"] for item in observed["issues"]} == {
        "unregistered_result_semantic_literal",
    }


def test_financial_oracle_cannot_interpret_unregistered_schema_or_path(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    target = source / "research_pipeline/evidence/result_financial_oracle.py"
    target.parent.mkdir(parents=True)
    target.write_text(
        'def verify(bundle):\n'
        '    if any(\n'
        '        table.schema_id == "research.generic-special-result.v1"\n'
        '        for table in bundle.tables\n'
        '    ):\n'
        '        return "project/output"\n',
        encoding="utf-8",
    )
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({
        "contract_version": "research-framework-boundary-v2",
        "scan_paths": ["."],
    }), encoding="utf-8")

    observed = audit_framework_boundary(
        tmp_path, source_root=source, policy_path=policy,
    )
    assert {item["code"] for item in observed["issues"]} == {
        "unregistered_result_semantic_literal",
    }


def test_financial_oracle_contract_projection_has_no_fixed_result_semantics(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    target = source / "research_pipeline/evidence/result_financial_oracle.py"
    target.parent.mkdir(parents=True)
    target.write_text(
        'def verify(bundle, *, semantic_contract):\n'
        '    return verify_tables(\n'
        '        bundle,\n'
        '        schema_roles=semantic_contract.schema_roles,\n'
        '        support_paths=semantic_contract.required_support_paths,\n'
        '    )\n',
        encoding="utf-8",
    )
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({
        "contract_version": "research-framework-boundary-v2",
        "scan_paths": ["."],
    }), encoding="utf-8")

    observed = audit_framework_boundary(
        tmp_path, source_root=source, policy_path=policy,
    )
    assert observed["status"] == "pass"


def test_registered_result_semantic_handler_owns_fixed_schema_and_path(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    target = source / "research_pipeline/evidence/verification_result.py"
    target.parent.mkdir(parents=True)
    target.write_text(
        'HANDLER = _ResultSemanticHandler(\n'
        '    "generic",\n'
        '    ("research.generic-project-output.v1",),\n'
        '    (("research.generic-project-output.v1", "project/output"),),\n'
        '    verifier_identity="result-semantic:generic:verifier.generic.v1",\n'
        ')\n',
        encoding="utf-8",
    )
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({
        "contract_version": "research-framework-boundary-v2",
        "scan_paths": ["."],
    }), encoding="utf-8")

    observed = audit_framework_boundary(
        tmp_path, source_root=source, policy_path=policy,
    )
    assert observed["status"] == "pass"


def test_valid_exemption_is_reported(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    file = source / "history.json"
    file.write_text("project_only", encoding="utf-8")
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({
        "contract_version": "research-framework-boundary-v1",
        "forbidden_import_prefixes": [],
        "forbidden_tokens": ["project_only"],
        "scan_paths": ["."],
        "exemptions": [{"path": "source/history.json", "token": "project_only", "reason": "archive", "owner": "test", "expires_at": "2099-12-31"}],
    }), encoding="utf-8")
    result = audit_framework_boundary(tmp_path, source_root=source, policy_path=policy)
    assert result["status"] == "pass"
    assert result["exemptions_used"] == [{"path": "source/history.json", "token": "project_only"}]


@pytest.mark.parametrize(
    ("source_text", "expected_code"),
    (
        (
            'graph = _load_graph_plan(plan_root, manifest)\n'
            'factor = _node(graph, "research.factor.standard")\n',
            "business_full_graph_access",
        ),
        (
            'for request_id, plan in admitted_plans.items():\n'
            '    role = roles.get(plan.query.dataset_id)\n',
            "business_unbound_plan_discovery",
        ),
        (
            'matches = [plan for plan in admitted_plans.values() '
            'if plan.query.dataset_id == "cn_index.daily_bar"]\n',
            "business_unbound_plan_discovery",
        ),
        (
            'def execute(context):\n'
            '    return execute_business(admitted_plans=context.environment.admitted_plans)\n',
            "business_full_admitted_plan_access",
        ),
        (
            'def execute(research_id):\n'
            '    if research_id == "other_study":\n'
            '        return 1\n',
            "fixed_public_identity",
        ),
        (
            'definition = _definition(\n'
            '    module_name="research_pipeline.runtime.generic",\n'
            '    dependency_modules=("project_extensions.study.verifier",),\n'
            ')\n',
            "project_module_in_public_implementation",
        ),
        (
            'def verify(payload, validation_mode):\n'
            '    if validation_mode == "locked_holdout_v1":\n'
            '        return payload["project_stage"], payload["reason_code"]\n',
            "mode_bound_project_stage_reason",
        ),
        (
            'GENERIC_SOURCE_FIELDS = {\n'
            '    "market": {"session": "date", "code": "symbol"},\n'
            '    "membership": {"session": "date", "code": "symbol"},\n'
            '    "valuation": {"session": "day", "code": "symbol"},\n'
            '}\n',
            "fixed_business_role_inventory",
        ),
        (
            'def build_default_payload():\n'
            '    """只供测试构造完整通过样本。"""\n'
            '    return {}\n',
            "test_only_helper_in_production",
        ),
    ),
)
def test_renamed_specialization_still_fails(
    tmp_path: Path, source_text: str, expected_code: str,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "business.py").write_text(source_text, encoding="utf-8")
    policy = tmp_path / "policy.json"
    _policy(policy, token="project_only")
    observed = audit_framework_boundary(
        tmp_path, source_root=source, policy_path=policy,
    )
    assert expected_code in {item["code"] for item in observed["issues"]}
    assert all(item["observed"] and item["remediation"] for item in observed["issues"])


def test_full_core_scan_and_file_scoped_scheduler_exemption(tmp_path: Path) -> None:
    source = tmp_path / "source"
    for directory in ("application", "packages", "data_plane", "platform", "catalog"):
        target = source / directory
        target.mkdir(parents=True)
        (target / "business.py").write_text(
            'execute_business(admitted_plans=environment.admitted_plans)\n',
            encoding="utf-8",
        )
    (source / "catalog" / "default_lock").mkdir()
    (source / "catalog" / "default_lock" / "old.json").write_text(
        '"project_only"', encoding="utf-8",
    )
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({
        "contract_version": "research-framework-boundary-v2",
        "scan_paths": ["."],
        "exclude_paths": ["catalog/default_lock"],
        "forbidden_tokens": ["project_only"],
        "structural_exemptions": [{
            "path": "source/packages/business.py",
            "rules": ["business_full_admitted_plan_access"],
            "reason": "测试中的调度器",
        }],
    }), encoding="utf-8")
    observed = audit_framework_boundary(
        tmp_path, source_root=source, policy_path=policy,
    )
    paths = {
        item["path"] for item in observed["issues"]
        if item["code"] == "business_full_admitted_plan_access"
    }
    assert paths == {
        f"source/{directory}/business.py"
        for directory in ("application", "data_plane", "platform", "catalog")
    }
    assert "forbidden_token" not in {item["code"] for item in observed["issues"]}


def test_schema_identity_and_explicit_request_projection_pass(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "business.py").write_text(
        'def execute(environment, parameters, manifest):\n'
        '    plans = {request_id: environment.admitted_plans[request_id] '
        'for request_id in parameters["request_ids"]}\n'
        '    return manifest["research_id"] == parameters["research_id"], plans\n',
        encoding="utf-8",
    )
    policy = tmp_path / "policy.json"
    _policy(policy)
    observed = audit_framework_boundary(
        tmp_path, source_root=source, policy_path=policy,
    )
    assert observed["status"] == "pass"


def test_framework_table_roles_are_not_project_source_inventory(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "contracts.py").write_text(
        'TABLE_COLUMNS = {\n'
        '    "orders": ("session", "code"),\n'
        '    "fills": ("session", "code"),\n'
        '    "positions": ("session", "code"),\n'
        '}\n',
        encoding="utf-8",
    )
    policy = tmp_path / "policy.json"
    _policy(policy)
    observed = audit_framework_boundary(
        tmp_path, source_root=source, policy_path=policy,
    )
    assert observed["status"] == "pass"
