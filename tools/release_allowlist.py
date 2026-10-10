"""正式发布核心文件、独立测试文件和构建输入的唯一清单。"""

from __future__ import annotations

import hashlib
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import tarfile
from typing import Iterable
from zipfile import ZIP_DEFLATED, ZipFile


CORE_ROOT_FILES = (
    "LICENSE", "MANIFEST.in", "NOTICE", "README.md", "pyproject.toml",
)
BUILD_SUPPORT_FILES = (
    "release/dependency-distributions.json",
    "scripts/verify_clean_wheel.ps1",
    "tools/build_release_artifacts.py",
    "tools/build_release_envelope.py",
    "tools/build_release_manifest.py",
    "tools/clean_wheel_receipt.py",
    "tools/release_allowlist.py",
    "tools/release_metadata.py",
    "tools/release_evidence_binding.py",
    "tools/wheel_source_inventory.py",
    "tools/run_release_gate_tests.py",
    "tools/run_release_workflows.py",
    "tools/build_gate_c_evidence.py",
    "release/gate-test-protocol.json",
    "release/gate-a-protocol.json",
)
UNIT_CORE_TEST_FILES = (
    "tests/test_atomic_directory.py",
    "tests/test_minute_public_inventory.py",
    "tests/test_project_operator_bundle.py",
    "tests/test_research_causal_time_contracts.py",
    "tests/test_runtime_checkpoint.py",
    "tests/test_simulation_semantics.py",
    "tests/validity_facts_support.py",
    "integrations/rdagent/tests/test_package_campaign_execution.py",
    "integrations/rdagent/tests/test_package_execution_isolation.py",
    "integrations/rdagent/tests/test_failure_classification.py",
)
PUBLIC_ROOT_FILES = (
    ".gitignore",
    "ARCHITECTURE.md",
    "CHANGELOG.md",
    "CODE_OF_CONDUCT.md",
    "CONTRIBUTING.md",
    "EXTENSIONS.md",
    "LICENSE",
    "MANIFEST.in",
    "NOTICE",
    "README.md",
    "SECURITY.md",
    "THIRD_PARTY_NOTICES.md",
    "pyproject.toml",
)
PUBLIC_DOC_FILES = (
    "docs/architecture/README.md",
    "docs/architecture/render.mjs",
    "docs/architecture/source-evidence.json",
    "docs/architecture/sources/architecture.json",
    "docs/architecture/sources/dataflow.json",
    "docs/architecture/sources/lifecycle.json",
    "docs/architecture/sources/sequence.json",
    "docs/architecture/sources/workflow.json",
    "docs/architecture/.gitignore",
    "docs/architecture/research-pipeline-architecture.html",
    "docs/architecture/dataflow-research-lineage.html",
    "docs/architecture/workflow-research-task.html",
    "docs/architecture/sequence-research-run.html",
    "docs/architecture/lifecycle-research-run.html",
    "docs/architecture/render-previews.mjs",
    "docs/architecture/sources/detailed/overview.json",
    "docs/architecture/sources/detailed/architecture.json",
    "docs/architecture/sources/detailed/dataflow.json",
    "docs/architecture/sources/detailed/workflow.json",
    "docs/architecture/sources/detailed/sequence.json",
    "docs/architecture/sources/detailed/lifecycle.json",
    "licenses/ARCHIFY-MIT.txt",
    "docs/architecture/overview.html",
    "docs/architecture/sources/overview.json",
    'docs/architecture/index.html',
    'docs/architecture/overview.svg',
    'docs/architecture/research-pipeline-architecture.svg',
    'docs/architecture/dataflow-research-lineage.svg',
    'docs/architecture/workflow-research-task.svg',
    'docs/architecture/sequence-research-run.svg',
    'docs/architecture/lifecycle-research-run.svg',
    'licenses/QLIB-MIT.txt',
    'licenses/RD-AGENT-MIT.txt',

    "release/daily-cash-local-admission.md",
    "project_extensions/dai_zhu_er_jiu_metrics.md",
    "docs/framework-contracts.md",
    "docs/ai_workflow.md",
    "docs/architecture.md",
    "docs/financial_simulation.md",
    "docs/spot_account.md",
    "docs/explicit_orders.md",
    "docs/minute_simulation.md",
    "docs/bar_tca.md",
    "docs/catalog.md",
    "docs/cli.md",
    "docs/data_plane.md",
    "docs/evidence.md",
    "docs/getting-started.md",
    "docs/workspace-quickstart.md",
    "docs/index.md",
    "docs/minute_rule_provenance.md",
    "docs/native_backtest_acceptance.md",
    "docs/shared_futures.md",
    "docs/operations.md",
    "docs/release.md",
    "docs/external-acceptance.md",
    "docs/research_package.md",
    "docs/runtime.md",
    "docs/walk_forward_model.md",
    "docs/qlib-model-results.md",
    "docs/qlib-report.md",
    "docs/qlib-expressions.md",
    "docs/project_resource_budgets.md",
    "project_extensions/README.md",
    "examples/README.md",
    "docs/qlib-factor-baselines.md",
    "docs/qlib-portfolio-risk.md",
)
CORE_DOC_FILES = (
    "examples/qlib_portfolio/README.md",
    "integrations/rdagent/README.md",
    "integrations/rdagent/docs/formula-reproduction.md",
    "integrations/rdagent/docs/factor-research.md",
    "integrations/rdagent/examples/factor_research/README.md",
    "integrations/rdagent/docs/installation.md",
    "integrations/rdagent/docs/research-campaign.md",
    "integrations/rdagent/examples/package_campaign/README.md",
    "integrations/rdagent/examples/prediction_campaign/README.md",
    "integrations/rdagent/examples/volume_concentration/README.md",

    "ARCHITECTURE.md",
    "CONTRIBUTING.md",
    "EXTENSIONS.md",
    "SECURITY.md",
    "THIRD_PARTY_NOTICES.md",
    *PUBLIC_DOC_FILES,
    "integrations/rdagent/docs/coding-knowledge.md",
    "integrations/rdagent/docs/model-research.md",
    "integrations/rdagent/examples/model_research/README.md",
    "examples/qlib_risk/README.md",
    "project_extensions/qlib_portfolio_risk/README.md",
    "integrations/rdagent/docs/joint-research.md",
    "integrations/rdagent/docs/dai-daily-research.md",
)
PUBLIC_EXAMPLE_PROJECTS = (
    "equity_cross_section",
    "etf_time_series",
    "event_study",
    "futures_term_structure",
)
PUBLIC_EXAMPLE_ROOT_FILES = (
    "examples/__init__.py",
    "examples/build_bundles.py",
    "examples/prepare_synthetic_environment.py",
    "examples/qlib_portfolio/README.md",
    "examples/qlib_portfolio/inputs.py",
    "examples/qlib_portfolio/input_config.py",
    "examples/qlib_portfolio/synthetic.py",
    "examples/qlib_portfolio/prepare.py",
    "examples/qlib_portfolio/portfolio_plan.py",
    "examples/qlib_portfolio/extension/portfolio.py",
    "examples/qlib_portfolio/verifier/portfolio.py",
    "examples/qlib_portfolio/run.py",
    "examples/qlib_portfolio/horizons.py",
    "examples/qlib_portfolio/extension/operator.py",
    "examples/qlib_portfolio/extension/expressions.py",
    "examples/qlib_portfolio/extension/validity.py",
    "examples/qlib_portfolio/verifier/check.py",
    "examples/qlib_portfolio/extension/factor_baselines.py",
    "examples/qlib_portfolio/extension/factor_baseline_oracle.py",
    "examples/qlib_risk/prepare.py",
    "examples/qlib_risk/README.md",
    "examples/qlib_risk/run.py",
    "project_extensions/qlib_portfolio_risk/extension.py",
    "project_extensions/qlib_portfolio_risk/optimizer.py",
    "project_extensions/qlib_portfolio_risk/oracle.py",
    "project_extensions/qlib_portfolio_risk/README.md",
)
_PUBLIC_EXAMPLE_SUFFIXES = frozenset({".md", ".py", ".yaml"})
PUBLIC_GITHUB_FILES = (
    ".github/workflows/architecture-pages.yml",
    ".github/CODEOWNERS.template",
    ".github/workflows/ci.yml",
    ".github/ISSUE_TEMPLATE/bug_report.yml",
    ".github/ISSUE_TEMPLATE/feature_request.yml",
    ".github/pull_request_template.md",
    ".github/workflows/publish.yml",
)
PUBLIC_GOVERNANCE_FILES = (
    ".github/CODEOWNERS",
)
PUBLIC_TEST_FILES = (
    *UNIT_CORE_TEST_FILES,
    "tests/test_daily_cash_artifact.py",
    "tests/test_model_portfolio_validity.py",
    "tests/test_qlib_portfolio_example.py",
    "tests/test_qlib_own_input.py",
    "tests/test_portfolio_report.py",
    "tests/test_qlib_development.py",
    "tests/test_qlib_expressions.py",
    "tests/test_training_recorder.py",
    "tests/test_walk_forward_model_mainline.py",
    "tests/test_model_validity.py",
    "tests/test_model_validity_observation_time.py",
    "tests/test_model_sequence_validity.py",
    "tests/test_qlib_sequence_example.py",
    "tests/test_qlib_rolling_horizons.py",
    "tests/test_research_locked_holdout.py",
    "tests/test_qlib_horizon_example.py",
    "tests/test_qlib_horizon_group.py",
    "tests/test_qlib_sequence_admission.py",
    "tests/test_qlib_generated_model.py",
    "tests/test_qlib_alpha_baselines.py",
    "tests/test_qlib_portfolio_risk.py",
    "tests/test_qlib_portfolio_risk_bundle.py",
    "tests/test_qlib_portfolio_risk_targets.py",
    "tests/test_qlib_model_integration.py",
    "tests/test_qlib_sequence_windows.py",
    "tests/test_qlib_gru_contract.py",
    "tests/test_qlib_gru_integration.py",
    "tests/test_qlib_lstm_contract.py",
    "tests/test_qlib_lstm_integration.py",
    "tests/test_qlib_transformer_contract.py",
    "tests/test_qlib_transformer_integration.py",
    "tests/test_qlib_sequence_runtime.py",
    "tests/test_qlib_report.py",
    "tests/mainline_boundary_helpers.py",
    "tests/test_current_documentation.py",
    "tests/test_mainline_architecture_boundary.py",
    "tests/test_framework_intrusion_gate.py",
    "tests/test_public_examples.py",
    "tests/test_public_operator_governance.py",
    "tests/test_public_release_gates.py",
    "tests/test_version_release_plan.py",
    "tests/test_public_source_inventory.py",
    "tests/test_release_metadata_ssot.py",
    "tests/test_workspace.py",
    "tests/test_workspace_flow.py",
    "tests/test_package_diagnostics.py",
    "tests/test_cli_summary.py",
    "tests/test_process_budget_advice.py",
    "tests/test_verifier_resource_diagnostics.py",
    "tests/test_dsr_formula.py",
    "tests/test_cli_failure_details.py",
    "tests/test_dependency_distribution_identity.py",
    "tests/test_dependency_identity_recovery.py",
    "tests/test_release_envelope.py",
    "tests/test_release_gate_runner.py",
    "tests/test_capability_release_evidence.py",
    "tests/test_release_workflows.py",
    "tests/test_gate_c_evidence.py",
    "tests/test_generic_lifecycle.py",
    "tests/test_result_bundle.py",
    "tests/test_financial_checkpoint_recovery.py",
    "tests/test_financial_values.py",
    "tests/test_cn_stock_simulation.py",
    "tests/test_cn_etf_simulation.py",
    "tests/test_cn_futures_simulation.py",
)
PUBLIC_INTEGRATION_FILES = (
    "integrations/rdagent/README.md",
    "integrations/rdagent/LICENSE",
    "integrations/rdagent/NOTICE",
    "integrations/rdagent/MANIFEST.in",
    "integrations/rdagent/docs/installation.md",
    "integrations/rdagent/docs/research-campaign.md",
    "integrations/rdagent/docs/formula-reproduction.md",
    "integrations/rdagent/docs/factor-research.md",
    "integrations/rdagent/examples/factor_research/README.md",
    "integrations/rdagent/pyproject.toml",
    "integrations/rdagent/requirements-linux.txt",
    "integrations/rdagent/src/quantwitness_rdagent/__init__.py",
    "integrations/rdagent/src/quantwitness_rdagent/__main__.py",
    "integrations/rdagent/src/quantwitness_rdagent/contracts.py",
    "integrations/rdagent/src/quantwitness_rdagent/scenario.py",
    "integrations/rdagent/src/quantwitness_rdagent/loop.py",
    "integrations/rdagent/src/quantwitness_rdagent/coder.py",
    "integrations/rdagent/src/quantwitness_rdagent/coding_knowledge.py",
    "integrations/rdagent/src/quantwitness_rdagent/execution.py",
    "integrations/rdagent/src/quantwitness_rdagent/feedback.py",
    "integrations/rdagent/src/quantwitness_rdagent/worker.py",
    "integrations/rdagent/src/quantwitness_rdagent/generation.py",
    "integrations/rdagent/src/quantwitness_rdagent/model_client.py",
    "integrations/rdagent/src/quantwitness_rdagent/source_materials.py",
    "integrations/rdagent/src/quantwitness_rdagent/formula_spec.py",
    "integrations/rdagent/src/quantwitness_rdagent/request_builder.py",
    "integrations/rdagent/src/quantwitness_rdagent/campaign.py",
    "integrations/rdagent/src/quantwitness_rdagent/campaign_data.py",
    "integrations/rdagent/src/quantwitness_rdagent/campaign_loop.py",
    "integrations/rdagent/src/quantwitness_rdagent/package_campaign.py",
    "integrations/rdagent/src/quantwitness_rdagent/package_execution.py",
    "integrations/rdagent/src/quantwitness_rdagent/factor_research.py",
    "integrations/rdagent/src/quantwitness_rdagent/research_knowledge.py",
    "integrations/rdagent/src/quantwitness_rdagent/direction_selection.py",
    "integrations/rdagent/src/quantwitness_rdagent/factor_loop.py",
    "integrations/rdagent/src/quantwitness_rdagent/native_research.py",
    "integrations/rdagent/src/quantwitness_rdagent/native_backend.py",
    "integrations/rdagent/examples/factor_research/prepare.py",
    "integrations/rdagent/tests/test_factor_research.py",
    "integrations/rdagent/tests/test_research_knowledge.py",
    "integrations/rdagent/tests/test_direction_selection.py",
    "integrations/rdagent/tests/test_factor_research_loop.py",
    "integrations/rdagent/tests/test_price_factor_loop.py",
    "integrations/rdagent/tests/test_coding_knowledge.py",
    "integrations/rdagent/tests/test_factor_research_example.py",
    "integrations/rdagent/tests/test_native_research.py",
    "integrations/rdagent/examples/package_campaign/README.md",
    "integrations/rdagent/examples/package_campaign/prepare.py",
    "integrations/rdagent/examples/package_campaign/prepare_qlib.py",
    "integrations/rdagent/tests/test_package_campaign.py",
    "integrations/rdagent/tests/test_package_campaign_execution.py",
    "integrations/rdagent/tests/test_package_execution_isolation.py",
    "integrations/rdagent/tests/test_failure_classification.py",
    "integrations/rdagent/tests/test_package_campaign_loop.py",
    "integrations/rdagent/examples/prediction_campaign/prepare.py",
    "integrations/rdagent/examples/prediction_campaign/README.md",
    "integrations/rdagent/tests/test_campaign_data.py",
    "integrations/rdagent/tests/test_campaign.py",
    "integrations/rdagent/tests/test_campaign_loop.py",
    "integrations/rdagent/examples/volume_concentration/README.md",
    "integrations/rdagent/examples/volume_concentration/prepare.py",
    "integrations/rdagent/examples/volume_concentration/check_recovery.py",
    "integrations/rdagent/examples/volume_concentration/operator.yaml",
    "integrations/rdagent/examples/volume_concentration/verifier.yaml",
    "integrations/rdagent/examples/volume_concentration/candidate/adapter.py",
    "integrations/rdagent/examples/volume_concentration/candidate/compute.py",
    "integrations/rdagent/examples/volume_concentration/verifier/check.py",
    "integrations/rdagent/tests/test_volume_concentration_example.py",
    "integrations/rdagent/tests/test_request_builder.py",
    "integrations/rdagent/tests/test_formula_spec.py",
    "integrations/rdagent/tests/test_source_materials.py",
    "integrations/rdagent/tests/test_live_generation.py",
    "integrations/rdagent/tests/test_request_and_recovery.py",
    "integrations/rdagent/docs/coding-knowledge.md",
    "integrations/rdagent/docs/model-research.md",
    "integrations/rdagent/examples/model_research/README.md",
    "integrations/rdagent/examples/model_research/prepare.py",
    "integrations/rdagent/src/quantwitness_rdagent/coding_transfer.py",
    "integrations/rdagent/src/quantwitness_rdagent/model_knowledge.py",
    "integrations/rdagent/src/quantwitness_rdagent/model_research_evidence.py",
    "integrations/rdagent/src/quantwitness_rdagent/native_model_research.py",
    "integrations/rdagent/src/quantwitness_rdagent/model_research.py",
    "integrations/rdagent/src/quantwitness_rdagent/model_loop.py",
    "integrations/rdagent/src/quantwitness_rdagent/model_execution.py",
    "integrations/rdagent/tests/test_model_coding_knowledge.py",
    "integrations/rdagent/tests/test_model_research.py",
    "integrations/rdagent/tests/test_native_model_research.py",
    "integrations/rdagent/docs/joint-research.md",
    "integrations/rdagent/examples/joint_research/prepare.py",
    "integrations/rdagent/src/quantwitness_rdagent/joint_feedback.py",
    "integrations/rdagent/src/quantwitness_rdagent/joint_loop.py",
    "integrations/rdagent/src/quantwitness_rdagent/joint_research.py",
    "integrations/rdagent/tests/test_joint_research.py",
    "integrations/rdagent/tests/test_daily_dai_contract.py",
    "integrations/rdagent/docs/dai-daily-research.md",
)
PUBLIC_TOOL_FILES = (
    *BUILD_SUPPORT_FILES,
    "tools/public_source_inventory.py",
    "tools/verify_rdagent_install.py",
    "tools/verify_ml_install.py",
    "tools/plan_version_release.py",
)
_FORBIDDEN_PARTS = {
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    "__pycache__",
    "build",
    "dist",
    "release-candidate",
}
_FORBIDDEN_SUFFIXES = (
    ".db",
    ".duckdb",
    ".duckdb.wal",
    ".log",
    ".parquet",
    ".pickle",
    ".pkl",
    ".pyc",
    ".sqlite",
    ".sqlite3",
)
_FORBIDDEN_RESEARCH_PACKAGE_PARTS = frozenset({
    "project_extensions",
    "research_packages",
    "retirement_migration",
})
_RETIRED_RESEARCH_MODULE_NAMES = frozenset({
    "artifact_lineage.py",
    "block_contracts.py",
    "block_runner.py",
    "candidate_reference_statistics.py",
    "candidate_reference_validity.py",
    "candidate_selection.py",
    "candidate_simulation_validity.py",
    "candidate_table.py",
    "factor_profile_execution.py",
    "factor_profile_oracle.py",
    "ff3_artifact_contracts.py",
    "ff3_oracle.py",
    "ff3_relations.py",
    "framework_binding.py",
    "monthly_labels.py",
    "parameter_targets.py",
    "parameterized_holdings.py",
    "pit_cross_sectional_panel.py",
    "random_stability_contracts.py",
    "risk_artifact_contracts.py",
    "risk_model.py",
    "risk_model_oracle.py",
    "runtime_artifact.py",
    "stock_target_simulation_support.py",
    "target_simulation.py",
})


def _require_project(project: Path) -> Path:
    root = project.resolve(strict=True)
    if (
        not (root / "pyproject.toml").is_file()
        or not (root / "src/research_pipeline").is_dir()
        or not (root / "src/factor_contracts").is_dir()
    ):
        raise ValueError(f"不是 research_pipeline 项目根: {root}")
    return root


def _validate_relative(path: str) -> str:
    relative = PurePosixPath(path)
    lowered = relative.as_posix().lower()
    if (
        relative.is_absolute()
        or ".." in relative.parts
        or any(part.lower() in _FORBIDDEN_PARTS for part in relative.parts)
        or lowered.endswith(_FORBIDDEN_SUFFIXES)
    ):
        raise ValueError(f"发布清单包含禁止路径: {path}")
    return relative.as_posix()


def reject_release_package_path(path: str | PurePosixPath) -> str:
    """拒绝项目源码、退休迁移和已下线专用模块进入正式包。"""

    normalized = _validate_relative(PurePosixPath(path).as_posix())
    relative = PurePosixPath(normalized)
    lowered_parts = tuple(part.lower() for part in relative.parts)
    try:
        package_index = lowered_parts.index("research_pipeline")
    except ValueError:
        return normalized
    package_parts = lowered_parts[package_index + 1 :]
    if any(part in _FORBIDDEN_RESEARCH_PACKAGE_PARTS for part in package_parts):
        raise ValueError(f"正式包禁止包含项目或退休目录: {normalized}")
    if relative.name.lower() in _RETIRED_RESEARCH_MODULE_NAMES:
        raise ValueError(f"正式包禁止包含已退休专用模块: {normalized}")
    return normalized


def current_catalog_lock_name(project: Path) -> str:
    root = _require_project(project)
    lock_root = root / "src/research_pipeline/catalog/default_lock"
    name = (lock_root / "CURRENT").read_text(encoding="utf-8").strip()
    if not name or PurePosixPath(name).name != name or not (lock_root / name).is_dir():
        raise ValueError("Catalog CURRENT 未指向唯一现有 lock 目录")
    return name


def package_file_paths(project: Path) -> tuple[str, ...]:
    """返回公开发行包文件；私有 Catalog 声明与 lock 必须由使用者显式提供。"""

    root = _require_project(project)
    result: list[str] = []
    for package_name in ("factor_contracts", "research_pipeline"):
        package_root = root / "src" / package_name
        for path in sorted(package_root.rglob("*")):
            if not path.is_file():
                continue
            relative = path.relative_to(root).as_posix()
            package_relative = path.relative_to(package_root).parts
            if (
                package_name == "research_pipeline"
                and package_relative[:2]
                in {("catalog", "default_lock"), ("catalog", "definitions")}
            ):
                continue
            if (
                package_name == "research_pipeline"
                and package_relative[:2] == ("domain", "rule_snapshots")
                and path.name not in {"minute_reference_rules.json", "minute_reference_rules.v5.json", "minute_reference_rules.v6.json"}
            ):
                continue
            if relative == "src/research_pipeline/platform/minute_capabilities/minute_inventory.v3.json":
                continue
            if (
                any(part.lower() in _FORBIDDEN_PARTS for part in PurePosixPath(relative).parts)
                or relative.lower().endswith(_FORBIDDEN_SUFFIXES)
            ):
                continue
            result.append(reject_release_package_path(relative))
    if not result:
        raise ValueError("正式 Python 包文件清单为空")
    return tuple(result)


def core_release_paths(project: Path) -> tuple[str, ...]:
    root = _require_project(project)
    required = (*CORE_ROOT_FILES, *CORE_DOC_FILES, *package_file_paths(root))
    missing = [path for path in required if not (root / path).is_file()]
    if missing:
        raise ValueError(f"正式发布核心文件缺失: {missing}")
    return tuple(sorted({_validate_relative(path) for path in required}))


def source_archive_paths(project: Path) -> tuple[str, ...]:
    root = _require_project(project)
    required = (*core_release_paths(root), *UNIT_CORE_TEST_FILES)
    missing = [path for path in required if not (root / path).is_file()]
    if missing:
        raise ValueError(f"独立测试源码文件缺失: {missing}")
    return tuple(sorted({_validate_relative(path) for path in required}))


def public_source_paths(project: Path) -> tuple[str, ...]:
    """返回独立公开仓库允许出现的文件，安装源码复用 package inventory。"""

    root = _require_project(project)
    required = (
        *PUBLIC_ROOT_FILES,
        *package_file_paths(root),
        *PUBLIC_DOC_FILES,
        *PUBLIC_EXAMPLE_ROOT_FILES,
        *PUBLIC_GITHUB_FILES,
        *PUBLIC_TEST_FILES,
        *PUBLIC_TOOL_FILES,
        *PUBLIC_INTEGRATION_FILES,
        "release/framework-boundary/policy.json",
    )
    governance = tuple(path for path in PUBLIC_GOVERNANCE_FILES if (root / path).is_file())
    missing = [path for path in required if not (root / path).is_file()]
    if missing:
        raise ValueError(f"公开源码清单文件缺失: {missing}")
    examples: list[str] = []
    for project_name in PUBLIC_EXAMPLE_PROJECTS:
        example_root = root / "examples" / project_name
        if not example_root.is_dir():
            raise ValueError(f"公开示例目录缺失: examples/{project_name}")
        paths = tuple(path for path in example_root.rglob("*") if path.is_file())
        if not paths:
            raise ValueError(f"公开示例目录为空: examples/{project_name}")
        for path in paths:
            relative = path.relative_to(root).as_posix()
            if (
                any(part.lower() in _FORBIDDEN_PARTS for part in path.parts)
                or relative.lower().endswith(_FORBIDDEN_SUFFIXES)
            ):
                continue
            if path.suffix.lower() not in _PUBLIC_EXAMPLE_SUFFIXES:
                raise ValueError(f"公开示例包含未允许文件: {relative}")
            examples.append(relative)
    result = tuple(sorted({
        _validate_relative(path) for path in (*required, *governance, *examples)
    }))
    forbidden_roots = (
        "replacement_acceptance/",
        "research_packages/",
        "retirement_migration/",
    )
    if any(path.startswith(forbidden_roots) for path in result):
        raise ValueError("公开源码清单包含内部项目、验收或迁移材料")
    return result


def build_input_paths(project: Path, dependency_lock: Path) -> tuple[str, ...]:
    root = _require_project(project)
    dependency = dependency_lock.resolve(strict=True)
    try:
        dependency_relative = dependency.relative_to(root).as_posix()
    except ValueError as exc:
        raise ValueError("dependency lock 必须位于项目 allowlist 内") from exc
    required = (
        *source_archive_paths(root),
        *BUILD_SUPPORT_FILES,
        *public_source_paths(root),
        dependency_relative,
    )
    missing = [path for path in required if not (root / path).is_file()]
    if missing:
        raise ValueError(f"构建输入缺失: {missing}")
    return tuple(sorted({_validate_relative(path) for path in required}))


def copy_paths(project: Path, destination: Path, paths: Iterable[str]) -> None:
    root = _require_project(project)
    target_root = destination.resolve()
    target_root.mkdir(parents=True, exist_ok=True)
    for relative in sorted(set(paths)):
        normalized = _validate_relative(relative)
        source = root / normalized
        if not source.is_file():
            raise ValueError(f"发布清单文件不存在: {normalized}")
        target = target_root / normalized
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)


def file_digests(project: Path, paths: Iterable[str]) -> dict[str, str]:
    root = _require_project(project)
    return {
        relative: hashlib.sha256((root / relative).read_bytes()).hexdigest()
        for relative in sorted(set(paths))
    }


def _is_current_package_path(path: str, *, current_lock: str | None) -> bool:
    normalized = _validate_relative(path)
    relative = PurePosixPath(normalized)
    if relative.parts[:2] == ("src", "factor_contracts"):
        return True
    package_prefix = PurePosixPath("src/research_pipeline")
    try:
        package_relative = relative.relative_to(package_prefix)
    except ValueError:
        return False
    if package_relative.parts[:2] != ("catalog", "default_lock"):
        return True
    tail = package_relative.parts[2:]
    return bool(tail) and tail[0] in {"CURRENT", current_lock}


def _tracked_package_paths(project: Path) -> set[str]:
    """返回 Git 已跟踪的当前包文件，包含工作区中已删除的文件。"""

    current_file = project / "src/research_pipeline/catalog/default_lock/CURRENT"
    current = current_catalog_lock_name(project) if current_file.is_file() else None
    completed = subprocess.run(
        [
            "git", "-C", str(project), "ls-files", "-z", "--",
            "src/factor_contracts", "src/research_pipeline",
        ],
        check=True,
        capture_output=True,
    )
    result: set[str] = set()
    for raw in completed.stdout.split(b"\0"):
        if not raw:
            continue
        candidate = raw.decode("utf-8", errors="surrogateescape").replace("\\", "/")
        if _is_current_package_path(candidate, current_lock=current):
            result.add(candidate)
    return result


def allowlisted_source_dirty(project: Path, paths: Iterable[str]) -> bool:
    root = _require_project(project)
    allowed = {_validate_relative(path) for path in paths}
    allowed.update(_tracked_package_paths(root))
    prefix = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "--show-prefix"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip().replace("\\", "/")
    status = subprocess.run(
        ["git", "-C", str(root), "status", "--porcelain=v1", "-z", "--untracked-files=all", "--", "."],
        check=True,
        capture_output=True,
    ).stdout
    records = status.split(b"\0")
    index = 0
    while index < len(records):
        raw = records[index]
        index += 1
        if not raw:
            continue
        entry = raw.decode("utf-8", errors="surrogateescape")
        if len(entry) < 4 or entry[2] != " ":
            raise ValueError("无法解析 Git 工作区状态")
        status_code = entry[:2]
        candidates = [entry[3:].replace("\\", "/")]
        if "R" in status_code or "C" in status_code:
            if index >= len(records) or not records[index]:
                raise ValueError("Git rename/copy 状态缺少源路径")
            candidates.append(
                records[index]
                .decode("utf-8", errors="surrogateescape")
                .replace("\\", "/")
            )
            index += 1
        for candidate in candidates:
            if prefix and candidate.startswith(prefix):
                candidate = candidate[len(prefix) :]
            if candidate in allowed:
                return True
    return False


def write_source_archive(project: Path, output: Path) -> Path:
    root = _require_project(project)
    destination = output.resolve()
    if destination.exists():
        raise ValueError("独立源码压缩包输出已存在")
    destination.parent.mkdir(parents=True, exist_ok=True)
    archive_root = "research_pipeline-source"
    with ZipFile(destination, "x", compression=ZIP_DEFLATED) as archive:
        for relative in source_archive_paths(root):
            archive.write(root / relative, f"{archive_root}/{relative}")
    return destination


def verify_source_archive(project: Path, archive_path: Path) -> dict[str, str]:
    root = _require_project(project)
    expected = file_digests(root, source_archive_paths(root))
    with ZipFile(archive_path) as archive:
        actual = {}
        for item in archive.infolist():
            if item.is_dir():
                continue
            path = PurePosixPath(item.filename)
            if len(path.parts) < 2 or path.parts[0] != "research_pipeline-source":
                raise ValueError(f"独立源码压缩包根目录无效: {item.filename}")
            relative = PurePosixPath(*path.parts[1:]).as_posix()
            _validate_relative(relative)
            actual[relative] = hashlib.sha256(archive.read(item)).hexdigest()
    if actual != expected:
        raise ValueError("独立源码压缩包与 allowlist 不一致")
    return actual


def verify_sdist_inventory(project: Path, sdist: Path) -> dict[str, str]:
    root = _require_project(project)
    expected = file_digests(root, core_release_paths(root))
    metadata = {
        "PKG-INFO",
        "setup.cfg",
        "src/quantwitness.egg-info/PKG-INFO",
        "src/quantwitness.egg-info/SOURCES.txt",
        "src/quantwitness.egg-info/dependency_links.txt",
        "src/quantwitness.egg-info/entry_points.txt",
        "src/quantwitness.egg-info/requires.txt",
        "src/quantwitness.egg-info/top_level.txt",
    }
    actual: dict[str, str] = {}
    with tarfile.open(sdist, "r:gz") as archive:
        for item in archive.getmembers():
            if not item.isfile():
                continue
            path = PurePosixPath(item.name)
            if len(path.parts) < 2:
                raise ValueError(f"sdist 根目录无效: {item.name}")
            relative = PurePosixPath(*path.parts[1:]).as_posix()
            if relative in metadata:
                continue
            _validate_relative(relative)
            handle = archive.extractfile(item)
            if handle is None:
                raise ValueError(f"sdist 文件无法读取: {item.name}")
            actual[relative] = hashlib.sha256(handle.read()).hexdigest()
    if actual != expected:
        missing = sorted(expected.keys() - actual.keys())
        extra = sorted(actual.keys() - expected.keys())
        changed = sorted(key for key in expected.keys() & actual.keys() if expected[key] != actual[key])
        raise ValueError(f"sdist 与 allowlist 不一致: missing={missing[:5]}, extra={extra[:5]}, changed={changed[:5]}")
    return actual


__all__ = [
    "BUILD_SUPPORT_FILES",
    "CORE_ROOT_FILES",
    "CORE_DOC_FILES",
    "UNIT_CORE_TEST_FILES",
    "allowlisted_source_dirty",
    "build_input_paths",
    "copy_paths",
    "core_release_paths",
    "current_catalog_lock_name",
    "file_digests",
    "package_file_paths",
    "public_source_paths",
    "reject_release_package_path",
    "source_archive_paths",
    "verify_sdist_inventory",
    "verify_source_archive",
    "write_source_archive",
]
