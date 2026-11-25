from __future__ import annotations

from dataclasses import replace
from datetime import datetime
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from research_pipeline.cli import build_parser
from research_pipeline.extensions import load_project_operator_declaration
from research_pipeline.extensions.project_scaffold import scaffold_project_operator
from research_pipeline.packages import load_research_package
from research_pipeline.packages.models import ResearchPackageError
from research_pipeline.packages.variants import expand_research_package_variants
from research_pipeline.application import research_run
from research_pipeline.platform import canonical_json, typed_canonical_hash
from research_pipeline.platform.operator_contracts import OperatorNodeRecipe
from research_pipeline.runtime import (
    AuditEnvironmentManifest,
    CheckpointPolicy,
    DagSpec,
    DeterminismContext,
    Edge,
    ExecutionMode,
    NodeSpec,
    PartitionSpec,
    ResourceBudget,
    RetryPolicy,
)
from research_pipeline.runtime.checkpoint import CheckpointStore
from research_pipeline.runtime.diagnostics import write_finalize_status
from research_pipeline.runtime.execution_service import (
    OPERATOR_DAG_RUN_VERSION,
    RuntimeExecutionService,
)
from research_pipeline.runtime.external_artifact import ExternalArtifactStore
from research_pipeline.runtime.failed_run_reuse import (
    FAILED_RUN_REUSE_MODE,
    prepare_failed_run_reuse,
)
from research_pipeline.runtime.required_run_reuse import prepare_required_run_reuse
from research_pipeline.runtime.errors import RuntimeIntegrityError
from research_pipeline.runtime.operator_registry import (
    NODE_IDENTITY_PROJECTION_CURRENT,
    _node_configuration_hash,
    _project_cross_run_cacheable,
)
from research_pipeline.runtime.operator_definitions import (
    build_mainline_operator_manifest,
)
from research_pipeline.runtime.operator_runtime import (
    RuntimeNodeOutputs,
    RuntimeNodeValue,
)
from research_pipeline.runtime.adapters.common import ResearchRunEnvironment
from research_pipeline.runtime.adapters.catalog import (
    execute_data_catalog_admission_v1,
)
from research_pipeline.runtime.store import EventStore


class _Query:
    def __init__(self, identity: str) -> None:
        self.identity = identity

    def to_dict(self) -> dict[str, str]:
        return {"identity": self.identity}


def test_node_local_identity_ignores_unconsumed_plan_facts() -> None:
    node = OperatorNodeRecipe("consumer", "research.consumer", "1.0.0", (), {"request_ids": ["used"]}, ())
    base = SimpleNamespace(
        request_ids=("used", "unused"),
        queries=(_Query("used-v1"), _Query("unused-v1")),
        root_seed=7,
        fixed_clock="2026-09-28T00:00:00+08:00",
        metric_proofs=("metric-a",),
        research_semantics=SimpleNamespace(semantics_hash="semantics-a"),
    )
    unrelated = SimpleNamespace(
        **{
            **base.__dict__,
            "queries": (_Query("used-v1"), _Query("unused-v2")),
            "metric_proofs": ("metric-b",),
            "research_semantics": SimpleNamespace(semantics_hash="semantics-b"),
        }
    )
    consumed = SimpleNamespace(
        **{
            **base.__dict__,
            "queries": (_Query("used-v2"), _Query("unused-v1")),
        }
    )
    assert _node_configuration_hash(base, node) == _node_configuration_hash(
        unrelated, node
    )
    assert _node_configuration_hash(base, node) != _node_configuration_hash(
        consumed, node
    )


def test_expand_variants_is_registered_under_package_command() -> None:
    args = build_parser().parse_args(
        [
            "package",
            "expand-variants",
            "--base",
            "base-package",
            "--manifest",
            "variants.yaml",
            "--output-root",
            "expanded",
        ]
    )
    assert args.command == "package"
    assert args.package_command == "expand-variants"


def test_catalog_admission_output_uses_node_local_identity(tmp_path: Path) -> None:
    parameters = {
        "request_ids": ("prices",),
        "catalog_hash": "a" * 64,
        "pit_contract_hash": "b" * 64,
        "drift_proof_hash": "c" * 64,
        "execution_estimates_hash": "d" * 64,
    }

    def output(package_plan_hash: str, configuration_hash: str):
        environment = ResearchRunEnvironment(
            plan_root=tmp_path,
            manifest={"package_plan_hash": package_plan_hash},
            admitted_plans={},
            database=tmp_path / "data.duckdb",
            source_databases={},
            minute_data_root=None,
            holdout_ledger_anchor=tmp_path / "holdout-ledger.json",
            node_parameters={"admission": parameters},
            semantics=None,
            fixed_clock="2026-09-28T00:00:00+08:00",
            root_seed=7,
            workers=1,
            resource_timeout_seconds=None,
            strategy_spec_hashes={},

        )
        context = SimpleNamespace(
            environment=environment,
            node=SimpleNamespace(
                node_id="admission",
                output_types=(("admission", "data.catalog-admission.v1"),),
                configuration_hash=configuration_hash,
            ),
        )
        return execute_data_catalog_admission_v1(context).values["admission"]

    first = output("1" * 64, "f" * 64)
    same_local_identity = output("2" * 64, "f" * 64)
    changed_local_identity = output("2" * 64, "0" * 64)
    assert first.artifact_ref == same_local_identity.artifact_ref
    assert first.inline_content == same_local_identity.inline_content
    assert first.artifact_ref != changed_local_identity.artifact_ref


def test_project_reuse_requires_explicit_declaration(tmp_path: Path) -> None:
    registered_specs = tuple(
        definition.operator_spec
        for definition in build_mainline_operator_manifest().definitions
    )
    extension = scaffold_project_operator(
        tmp_path / "extension",
        project_id="neutral-reuse-contract",
        operator_id="project.identity.reuse-contract",
        operator_version="1.0.0",
        registered_operator_specs=registered_specs,
    )
    legacy = load_project_operator_declaration(
        extension.declaration_path,
        source_root=extension.source_root,
    )
    opted_in_path = tmp_path / "cross-run.operator.yaml"
    opted_in_payload = yaml.safe_load(
        extension.declaration_path.read_text(encoding="utf-8")
    )
    opted_in_payload["reuse_scope"] = "cross_run"
    opted_in_path.write_text(
        yaml.safe_dump(opted_in_payload, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    opted_in = load_project_operator_declaration(
        opted_in_path,
        source_root=extension.source_root,
    )
    assert legacy.reuse_scope == "same_run"
    assert opted_in.reuse_scope == "cross_run"


def test_project_cross_run_cacheability_requires_every_generic_condition() -> None:
    manifest = SimpleNamespace(
        reuse_scope="cross_run",
        permissions=SimpleNamespace(artifact_write_scope="output_only"),
        operator_spec=SimpleNamespace(
            determinism_mode="deterministic",
            seed_policy="none",
        ),
    )
    assert _project_cross_run_cacheable(manifest)
    manifest.reuse_scope = "same_run"
    assert not _project_cross_run_cacheable(manifest)


@pytest.mark.parametrize("version", range(10, 16))
def test_legacy_invocation_is_rejected(tmp_path: Path, version: int) -> None:
    root = tmp_path / "run"
    args = SimpleNamespace(
        plan=str(tmp_path / "plan"),
        data_db=str(tmp_path / "data.duckdb"),
        source_db=[],
        minute_data_root=None,
        artifact_root=str(tmp_path / "artifacts"),
        handoff_out=str(tmp_path / "handoff.json"),
        run_root=str(root),
        result_store=str(tmp_path / "results"),
        mode="deterministic_serial",
        workers=1,
        root_seed=7,
        clock="2026-09-28T00:00:00+08:00",
        acceptance_proof=None,
        execution_engine="unified",
        resource_state_dir=None,
        runtime_rerun_parent_root=None,
        reuse_run_root=[],
        require_reused_node=[],
    )
    research_run._write_or_verify_runtime_invocation(root, args)
    path = root / "operator-dag-invocation.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    document.pop("invocation_hash")
    document.pop("reuse_run_roots")
    document["contract_version"] = f"research-operator-dag-invocation-v{version}"
    document["handoff_out"] = str(Path(args.handoff_out).resolve())
    path.write_text(
        canonical_json(
            {**document, "invocation_hash": typed_canonical_hash(document)}
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="invocation 版本"):
        research_run._load_operator_invocation(root)


def test_required_reuse_is_persisted_in_runtime_invocation(tmp_path: Path) -> None:
    root = tmp_path / "run"
    args = SimpleNamespace(
        plan=str(tmp_path / "plan"),
        data_db=str(tmp_path / "data.duckdb"),
        source_db=[],
        minute_data_root=None,
        artifact_root=str(tmp_path / "artifacts"),
        handoff_out=str(tmp_path / "handoff.json"),
        run_root=str(root),
        result_store=str(tmp_path / "results"),
        mode="deterministic_serial",
        workers=1,
        root_seed=7,
        clock="2026-09-28T00:00:00+08:00",
        acceptance_proof=None,
        execution_engine="unified",
        resource_state_dir=None,
        runtime_rerun_parent_root=None,
        reuse_run_root=[str(tmp_path / "source")],
        reuse_failed_run_root=None,
        require_reused_node=["shared"],
    )

    research_run._write_or_verify_runtime_invocation(root, args)

    loaded = research_run._load_operator_invocation(root)
    assert loaded.reuse_run_root == [str((tmp_path / "source").resolve())]
    assert loaded.require_reused_node == ["shared"]


def test_failed_run_reuse_imports_only_verified_successful_prefix(
    tmp_path: Path,
) -> None:
    service, original_node, context = _runtime_fixture()
    completed_node = replace(original_node, cacheable=False, pure=False)
    failed_node = NodeSpec(
        "failed",
        original_node.implementation_id,
        (("in", "test.output.v1"),),
        (("result", "test.result.v1"),),
        original_node.resource_budget,
        original_node.retry_policy,
        original_node.checkpoint_policy,
        original_node.partition,
        cacheable=False,
        pure=False,
        configuration_hash="d" * 64,
    )
    dag = DagSpec(
        "failed-reuse",
        (completed_node, failed_node),
        (
            Edge(
                completed_node.node_id,
                "out",
                failed_node.node_id,
                "in",
                "test.output.v1",
            ),
        ),
    )
    source = tmp_path / "source"
    source.mkdir()
    external = ExternalArtifactStore(source / "external-artifacts")
    staging = external.prepare()
    (staging / "value.txt").write_text("stable", encoding="utf-8")
    commit = external.commit(
        staging,
        artifact_name="out",
        artifact_type="test.output.v1",
    )
    outputs = RuntimeNodeOutputs.single(RuntimeNodeValue.external(commit))
    identity, expectation = service._identity(completed_node, (), context)
    CheckpointStore(source).commit_bytes(
        expectation=expectation,
        attempt_id="source-attempt-1",
        content=outputs.checkpoint_bytes(),
        outputs=outputs.artifact_refs,
        audit_environment_digest=service.audit_environment.manifest_digest,
        execution_identity_digest=identity.identity_digest,
        root_seed=context.root_seed,
        fixed_clock=context.fixed_clock.isoformat(),
    )
    run_id = "failed-source-run"
    events = EventStore(source)
    for status in ("created", "planned", "running"):
        service._status(events, run_id, "run", status, command_id=f"run:{status}")
    service._reconcile_success(
        events,
        run_id,
        completed_node.node_id,
        identity.node_execution_id,
    )
    service._status(
        events,
        run_id,
        "run",
        "failed",
        command_id="run:failed:failed",
    )
    record = {
        "contract_version": OPERATOR_DAG_RUN_VERSION,
        "status": "failed",
        "project_id": "source-project",
        "run_id": run_id,
        "dag_id": dag.dag_id,
        "dag": dag.to_dict(),
        "root_seed": context.root_seed,
        "fixed_clock": context.fixed_clock.isoformat(),
        "mode": ExecutionMode.DETERMINISTIC_SERIAL.value,
        "audit_environment": service.audit_environment.to_dict(),
        "audit_manifest_digest": service.audit_environment.manifest_digest,
        "node_identity_projection": NODE_IDENTITY_PROJECTION_CURRENT,
    }
    (source / "operator-dag-run.json").write_text(
        canonical_json(record),
        encoding="utf-8",
    )

    target = tmp_path / "target"
    recovery = prepare_failed_run_reuse(
        service=service,
        dag=dag,
        source_run_root=source,
        target_run_root=target,
        project_id="target-project",
        root_seed=context.root_seed,
        fixed_clock=context.fixed_clock.isoformat(),
        mode=ExecutionMode.DETERMINISTIC_SERIAL,
    )

    assert recovery["reuse_mode"] == FAILED_RUN_REUSE_MODE
    assert recovery["reuse_nodes"] == [completed_node.node_id]
    assert recovery["rerun_from_node"] == failed_node.node_id
    CheckpointStore(target, create=False).verify(expectation)


def test_failed_run_reuse_rejects_changed_dag(tmp_path: Path) -> None:
    service, node, context = _runtime_fixture()
    source = tmp_path / "source"
    source.mkdir()
    run_id = "failed-source-run"
    events = EventStore(source)
    for status in ("created", "planned", "running"):
        service._status(events, run_id, "run", status, command_id=f"run:{status}")
    service._status(
        events,
        run_id,
        "run",
        "failed",
        command_id="run:failed:finalize",
    )
    record = {
        "contract_version": OPERATOR_DAG_RUN_VERSION,
        "status": "failed",
        "run_id": run_id,
        "dag": DagSpec("source", (node,), ()).to_dict(),
        "root_seed": context.root_seed,
        "fixed_clock": context.fixed_clock.isoformat(),
        "mode": ExecutionMode.DETERMINISTIC_SERIAL.value,
        "audit_environment": service.audit_environment.to_dict(),
        "audit_manifest_digest": service.audit_environment.manifest_digest,
        "node_identity_projection": NODE_IDENTITY_PROJECTION_CURRENT,
    }
    (source / "operator-dag-run.json").write_text(
        canonical_json(record),
        encoding="utf-8",
    )
    changed = replace(node, configuration_hash="d" * 64)

    with pytest.raises(RuntimeIntegrityError, match="DAG、节点身份环境"):
        prepare_failed_run_reuse(
            service=service,
            dag=DagSpec("source", (changed,), ()),
            source_run_root=source,
            target_run_root=tmp_path / "target",
            project_id="target-project",
            root_seed=context.root_seed,
            fixed_clock=context.fixed_clock.isoformat(),
            mode=ExecutionMode.DETERMINISTIC_SERIAL,
        )


def test_cross_run_checkpoint_is_copied_after_full_verification(tmp_path: Path, monkeypatch) -> None:
    service, node, context = _runtime_fixture()
    source = tmp_path / "source"
    source_outputs, expectation, identity = _completed_source(
        service, node, context, source
    )
    target = tmp_path / "target"
    target_checkpoints = CheckpointStore(target)
    target_external = ExternalArtifactStore(target / "external-artifacts")
    sources = service._open_reuse_sources(
        (source,),
        target_root=target,
        node_identity_projection=NODE_IDENTITY_PROJECTION_CURRENT,
    )
    source_reads = []
    import research_pipeline.runtime.external_artifact as external_module
    original_hash = external_module._sha256
    def hash_file(path):
        if source in path.parents:
            source_reads.append(path)
        return original_hash(path)
    monkeypatch.setattr(external_module, "_sha256", hash_file)
    reused = service._reuse_cross_run_checkpoint(
        node=node,
        expectation=expectation,
        identity=identity,
        sources=sources,
        target_checkpoints=target_checkpoints,
        target_external=target_external,
        root_seed=context.root_seed,
        fixed_clock=context.fixed_clock.isoformat(),
    )
    assert reused is not None
    _source_run_id, _manifest, target_outputs = reused
    assert target_outputs.artifact_refs == source_outputs.artifact_refs
    target_checkpoints.verify(expectation)
    commit = next(iter(target_outputs.values.values())).external_commit
    assert commit is not None
    target_external.verify(commit.semantic_hash)
    assert source_reads == [source / "external-artifacts" / "objects" / commit.semantic_hash / "value.txt"]


def test_framework_audit_only_change_reuses_completed_checkpoint(tmp_path: Path, monkeypatch) -> None:
    source_service, node, context = _runtime_fixture()
    source = tmp_path / "source"
    _, source_expectation, source_identity = _completed_source(source_service, node, context, source)
    audit = replace(source_service.audit_environment, build_artifact_digest="e" * 64)
    target_service = RuntimeExecutionService(audit_environment=audit, numerical_backend_names=("numpy",))
    target_identity, target_expectation = target_service._identity(node, (), context)
    assert target_identity.audit_manifest_digest != source_identity.audit_manifest_digest
    assert target_expectation == source_expectation
    result = target_service.execute(
        dag=DagSpec("semantic-reuse", (node,), ()), environment=object(),
        run_root=tmp_path / "target", project_id="target-project",
        root_seed=context.root_seed, fixed_clock=context.fixed_clock.isoformat(),
        reuse_run_roots=(source,),
    )
    assert result["reused_nodes"] == [node.node_id]

    from research_pipeline.runtime import execution_service

    monkeypatch.setattr(execution_service, "FRAMEWORK_SEMANTIC_VERSION", "changed")
    _, changed_rule = target_service._identity(node, (), context)
    assert changed_rule.node_execution_id != source_expectation.node_execution_id
    monkeypatch.setenv("QUANTWITNESS_STRICT_AUDIT", "1")
    _, strict_source = source_service._identity(node, (), context)
    _, strict_target = target_service._identity(node, (), context)
    assert strict_source.node_execution_id != strict_target.node_execution_id


def test_cross_run_import_rechecks_source_on_next_call(tmp_path: Path, monkeypatch) -> None:
    service, node, context = _runtime_fixture()
    source = tmp_path / "source"
    source_outputs, expectation, identity = _completed_source(service, node, context, source)
    commit = next(iter(source_outputs.values.values())).external_commit
    sources = service._open_reuse_sources(
        (source,), target_root=tmp_path / "first",
        node_identity_projection=NODE_IDENTITY_PROJECTION_CURRENT,
    )
    reads = []
    original = ExternalArtifactStore._verify_uncached
    def verify(store, path, key):
        if store.root == source / "external-artifacts":
            reads.append(key)
        return original(store, path, key)
    monkeypatch.setattr(ExternalArtifactStore, "_verify_uncached", verify)
    def reuse(target):
        return service._reuse_cross_run_checkpoint(
            node=node, expectation=expectation, identity=identity, sources=sources,
            target_checkpoints=CheckpointStore(target),
            target_external=ExternalArtifactStore(target / "external-artifacts"),
            root_seed=context.root_seed, fixed_clock=context.fixed_clock.isoformat(),
        )
    assert reuse(tmp_path / "first") is not None
    assert reuse(tmp_path / "second") is not None
    assert reads == [commit.semantic_hash, commit.semantic_hash]
    (source / "external-artifacts" / "objects" / commit.semantic_hash / "value.txt").write_text(
        "tampered", encoding="utf-8",
    )
    with pytest.raises(RuntimeIntegrityError, match="漂移"):
        reuse(tmp_path / "third")
    assert reads == [commit.semantic_hash] * 3


def test_required_cross_run_reuse_passes_before_worker_start(tmp_path: Path) -> None:
    service, node, context = _runtime_fixture()
    source = tmp_path / "source"
    _completed_source(service, node, context, source)
    target = tmp_path / "target"

    preparation = prepare_required_run_reuse(
        service=service,
        dag=DagSpec("required-reuse", (node,), ()),
        source_run_roots=(source,),
        target_run_root=target,
        root_seed=context.root_seed,
        fixed_clock=context.fixed_clock.isoformat(),
        required_node_ids=(node.node_id,),
        node_identity_projection=NODE_IDENTITY_PROJECTION_CURRENT,
    )

    assert preparation["required_nodes"] == [node.node_id]
    assert preparation["prepared_nodes"] == [node.node_id]
    _, expectation = service._identity(node, (), context)
    CheckpointStore(target, create=False).verify(expectation)
    assert json.loads(
        (target / "required-reuse-plan.json").read_text(encoding="utf-8")
    ) == preparation

    result = service.execute(
        dag=DagSpec("required-reuse", (node,), ()),
        environment=object(),
        run_root=target,
        project_id="target-project",
        root_seed=context.root_seed,
        fixed_clock=context.fixed_clock.isoformat(),
        reuse_run_roots=(source,),
        node_identity_projection=NODE_IDENTITY_PROJECTION_CURRENT,
    )
    assert result["status"] == "succeeded"
    assert result["reused_nodes"] == [node.node_id]


def test_required_cross_run_reuse_accepts_explicit_non_cacheable_node(
    tmp_path: Path,
) -> None:
    service, original_node, context = _runtime_fixture()
    node = replace(original_node, cacheable=False, pure=False)
    source = tmp_path / "source"
    _completed_source(service, node, context, source)
    target = tmp_path / "target"

    preparation = prepare_required_run_reuse(
        service=service,
        dag=DagSpec("required-explicit-reuse", (node,), ()),
        source_run_roots=(source,),
        target_run_root=target,
        root_seed=context.root_seed,
        fixed_clock=context.fixed_clock.isoformat(),
        required_node_ids=(node.node_id,),
        node_identity_projection=NODE_IDENTITY_PROJECTION_CURRENT,
    )

    assert preparation["prepared_nodes"] == [node.node_id]
    _, expectation = service._identity(node, (), context)
    CheckpointStore(target, create=False).verify(expectation)


def test_required_cross_run_reuse_fails_before_worker_start_on_identity_drift(
    tmp_path: Path,
) -> None:
    service, node, context = _runtime_fixture()
    source = tmp_path / "source"
    _completed_source(service, node, context, source)
    changed = replace(node, configuration_hash="d" * 64)
    target = tmp_path / "target"

    with pytest.raises(RuntimeIntegrityError, match="启动前预检"):
        prepare_required_run_reuse(
            service=service,
            dag=DagSpec("required-reuse", (changed,), ()),
            source_run_roots=(source,),
            target_run_root=target,
            root_seed=context.root_seed,
            fixed_clock=context.fixed_clock.isoformat(),
            required_node_ids=(changed.node_id,),
            node_identity_projection=NODE_IDENTITY_PROJECTION_CURRENT,
        )

    assert not (target / "events.jsonl").exists()
    assert not (target / "operator-dag-run.json").exists()


def test_cross_run_candidate_corruption_fails_closed(tmp_path: Path) -> None:
    service, node, context = _runtime_fixture()
    source = tmp_path / "source"
    source_outputs, expectation, identity = _completed_source(
        service, node, context, source
    )
    commit = next(iter(source_outputs.values.values())).external_commit
    assert commit is not None
    payload = source / "external-artifacts" / "objects" / commit.semantic_hash / "value.txt"
    payload.write_text("tampered", encoding="utf-8")
    target = tmp_path / "target"
    sources = service._open_reuse_sources(
        (source,),
        target_root=target,
        node_identity_projection=NODE_IDENTITY_PROJECTION_CURRENT,
    )
    with pytest.raises(RuntimeIntegrityError, match="漂移"):
        service._reuse_cross_run_checkpoint(
            node=node,
            expectation=expectation,
            identity=identity,
            sources=sources,
            target_checkpoints=CheckpointStore(target),
            target_external=ExternalArtifactStore(target / "external-artifacts"),
            root_seed=context.root_seed,
            fixed_clock=context.fixed_clock.isoformat(),
        )


def test_expand_variants_is_atomic_and_produces_ordinary_packages(
    tmp_path: Path,
) -> None:
    base = (
        Path(__file__).parents[1]
        / "research_packages"
        / "references"
        / "c2_etf_time_series_momentum"
    )
    base_package = load_research_package(base)
    manifest = tmp_path / "variants.yaml"
    manifest.write_text(
        yaml.safe_dump(
            {
                "contract_version": "research-package-variant-set-v1",
                "base_package_hash": base_package.package_hash,
                "variants": [
                    _variant("short-window", "neutral_short", "neutral.graph.short", 10),
                    _variant("long-window", "neutral_long", "neutral.graph.long", 40),
                ],
            },
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    output = tmp_path / "expanded"
    result = expand_research_package_variants(
        base_root=base,
        manifest_path=manifest,
        output_root=output,
    )
    assert len(result["variants"]) == 2
    for slug, expected in (("neutral_short", 10), ("neutral_long", 40)):
        package = load_research_package(output / slug)
        nodes = package.spec_payload["graph"]["nodes"]
        signal = next(node for node in nodes if node["node_id"] == "signal")
        assert signal["parameters"]["lookback_sessions"] == expected


def test_expand_variants_reports_all_errors_without_partial_output(
    tmp_path: Path,
) -> None:
    base = (
        Path(__file__).parents[1]
        / "research_packages"
        / "references"
        / "c2_etf_time_series_momentum"
    )
    base_package = load_research_package(base)
    manifest = tmp_path / "invalid.yaml"
    invalid_node = _variant("bad-node", "bad_node", "neutral.graph.bad-node", 10)
    invalid_node["parameter_overrides"][0]["node_id"] = "missing"
    invalid_type = _variant("bad-type", "bad_type", "neutral.graph.bad-type", "ten")
    manifest.write_text(
        yaml.safe_dump(
            {
                "contract_version": "research-package-variant-set-v1",
                "base_package_hash": base_package.package_hash,
                "variants": [invalid_node, invalid_type],
            },
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    output = tmp_path / "expanded"
    with pytest.raises(ResearchPackageError) as captured:
        expand_research_package_variants(
            base_root=base,
            manifest_path=manifest,
            output_root=output,
        )
    message = str(captured.value)
    assert "variants[0]" in message and "variants[1]" in message
    assert not output.exists()


def _runtime_fixture():
    audit = AuditEnvironmentManifest.capture(
        build_artifact_digest="a" * 64,
        dependency_distribution_digests={"numpy": "b" * 64},
    )
    service = RuntimeExecutionService(
        audit_environment=audit,
        numerical_backend_names=("numpy",),
    )
    node = NodeSpec(
        "shared",
        "data.catalog.admission.v1",
        (),
        (("out", "test.output.v1"),),
        ResourceBudget(1024, 1, 0, 30),
        RetryPolicy(1, ()),
        CheckpointPolicy.REQUIRED,
        PartitionSpec(False),
        cacheable=True,
        pure=True,
        configuration_hash="c" * 64,
    )
    context = DeterminismContext(
        7,
        datetime.fromisoformat("2026-09-28T00:00:00+08:00"),
    )
    return service, node, context


def _completed_source(service, node, context, source: Path):
    source.mkdir()
    store = ExternalArtifactStore(source / "external-artifacts")
    staging = store.prepare()
    (staging / "value.txt").write_text("stable", encoding="utf-8")
    (port, artifact_type), = node.output_types
    commit = store.commit(staging, artifact_name=port, artifact_type=artifact_type)
    outputs = RuntimeNodeOutputs.single(RuntimeNodeValue.external(commit))
    identity, expectation = service._identity(node, (), context)
    CheckpointStore(source).commit_bytes(
        expectation=expectation,
        attempt_id="source-attempt-1",
        content=outputs.checkpoint_bytes(),
        outputs=outputs.artifact_refs,
        audit_environment_digest=service.audit_environment.manifest_digest,
        execution_identity_digest=identity.identity_digest,
        root_seed=context.root_seed,
        fixed_clock=context.fixed_clock.isoformat(),
    )
    run_id = "source-run"
    events = EventStore(source)
    for status in ("created", "planned", "running"):
        service._status(events, run_id, "run", status, command_id=f"run:{status}")
    service._reconcile_success(events, run_id, node.node_id, identity.node_execution_id)
    service._status(events, run_id, "run", "succeeded", command_id="run:succeeded")
    record = {
        "contract_version": OPERATOR_DAG_RUN_VERSION,
        "status": "succeeded",
        "run_id": run_id,
        "node_identity_projection": NODE_IDENTITY_PROJECTION_CURRENT,
        "event_chain_head": events.replay().chain_head,
    }
    (source / "operator-dag-run.json").write_text(
        canonical_json(record), encoding="utf-8"
    )
    result_directory = source / "published-result"
    result_directory.mkdir()
    write_finalize_status(source, status="pending")
    write_finalize_status(
        source,
        status="succeeded",
        result_id="result-id",
        result_directory=str(result_directory),
        result_published=True,
    )
    return outputs, expectation, identity


def _variant(
    variant_id: str,
    package_slug: str,
    graph_id: str,
    lookback: object,
) -> dict[str, object]:
    return {
        "variant_id": variant_id,
        "package_slug": package_slug,
        "display_name": package_slug,
        "package_version": "1.0.0",
        "graph_id": graph_id,
        "parameter_overrides": [
            {
                "node_id": "signal",
                "parameter_name": "lookback_sessions",
                "value": lookback,
            }
        ],
    }


@pytest.mark.parametrize("archived", [False, True])
def test_strict_failed_reuse_invocation_round_trip(tmp_path: Path, archived: bool) -> None:
    root = tmp_path / "run"
    args = SimpleNamespace(
        plan=str(tmp_path / "plan"),
        data_db=None if archived else str(tmp_path / "data.duckdb"),
        source_db=[], minute_data_root=None,
        input_snapshot_manifest=str(tmp_path / "snapshot.json") if archived else None,
        artifact_root=str(tmp_path / "artifacts"),
        handoff_out=str(tmp_path / "handoff.json"), run_root=str(root),
        result_store=str(tmp_path / "results"), mode="deterministic_serial", workers=1,
        root_seed=7, clock="2026-09-28T00:00:00+08:00", execution_engine="unified",
        resource_state_dir=None, runtime_rerun_parent_root=None, reuse_run_root=[],
        reuse_failed_run_root=str(tmp_path / "failed"), require_reused_node=["model"],
    )
    research_run._write_or_verify_runtime_invocation(root, args)
    loaded = research_run._load_operator_invocation(root)
    assert loaded.require_reused_node == ["model"]
    assert loaded.reuse_failed_run_root == str((tmp_path / "failed").resolve())
    assert loaded.reuse_run_root == []
    research_run._write_or_verify_runtime_invocation(root, loaded)
    loaded.require_reused_node = ["other"]
    with pytest.raises(ValueError, match="首次调用不一致"):
        research_run._write_or_verify_runtime_invocation(root, loaded)


def _strict_failed_source(tmp_path: Path, *, wrong_commit_event: bool = False):
    service, base, context = _runtime_fixture()
    source = tmp_path / "source"
    source.mkdir()
    root = replace(base, cacheable=False, pure=False)
    model = replace(root, node_id="model", input_types=(("in", "test.output.v1"),))
    validity = replace(model, node_id="validity", configuration_hash="d" * 64)
    other = replace(root, node_id="other")
    dag = DagSpec("original", (root, other, model, validity), (
        Edge("shared", "out", "model", "in", "test.output.v1"),
        Edge("model", "out", "validity", "in", "test.output.v1"),
    ))
    external = ExternalArtifactStore(source / "external-artifacts")
    checkpoints = CheckpointStore(source)
    events = EventStore(source)
    run_id = "failed-source-run"
    for status in ("created", "planned", "running"):
        service._status(events, run_id, "run", status, command_id=f"run:{status}")
    values = {}
    expectations = {}
    for node in (root, other, model):
        staging = external.prepare()
        (staging / "value.txt").write_text(node.node_id, encoding="utf-8")
        commit = external.commit(staging, artifact_name="out", artifact_type="test.output.v1")
        outputs = RuntimeNodeOutputs.single(RuntimeNodeValue.external(commit))
        inputs = service._inputs(dag, node, values)
        identity, expectation = service._identity(
            node, tuple(value.artifact_ref for _, value in sorted(inputs.items())), context,
        )
        checkpoints.commit_bytes(
            expectation=expectation, attempt_id=f"source-{node.node_id}",
            content=outputs.checkpoint_bytes(), outputs=outputs.artifact_refs,
            audit_environment_digest=service.audit_environment.manifest_digest,
            execution_identity_digest=identity.identity_digest,
            root_seed=context.root_seed, fixed_clock=context.fixed_clock.isoformat(),
        )
        committed_id = "f" * 64 if wrong_commit_event and node.node_id == "model" else identity.node_execution_id
        service._reconcile_success(events, run_id, node.node_id, committed_id)
        values[node.node_id] = outputs
        expectations[node.node_id] = expectation
    service._status(events, run_id, "run", "failed", command_id="run:failed")
    record = {
        "contract_version": OPERATOR_DAG_RUN_VERSION, "status": "failed", "run_id": run_id,
        "dag": dag.to_dict(), "root_seed": context.root_seed,
        "fixed_clock": context.fixed_clock.isoformat(), "mode": "deterministic_serial",
        "audit_environment": service.audit_environment.to_dict(),
        "audit_manifest_digest": service.audit_environment.manifest_digest,
        "node_identity_projection": NODE_IDENTITY_PROJECTION_CURRENT,
    }
    (source / "operator-dag-run.json").write_text(canonical_json(record), encoding="utf-8")
    changed = replace(dag, name="new-validity", nodes=(
        root, model, replace(validity, configuration_hash="e" * 64),
    ))
    kwargs = dict(
        service=service, dag=changed, source_run_root=source, target_run_root=tmp_path / "target",
        project_id="target-project", root_seed=context.root_seed,
        fixed_clock=context.fixed_clock.isoformat(), mode=ExecutionMode.DETERMINISTIC_SERIAL,
        required_node_ids=("model",),
    )
    return kwargs, expectations, values


def test_strict_failed_reuse_changes_only_unrequired_dag_and_reuses_ancestors(tmp_path: Path) -> None:
    kwargs, expectations, _ = _strict_failed_source(tmp_path)
    source = kwargs["source_run_root"]
    before = {str(path.relative_to(source)): path.read_bytes() for path in source.rglob("*") if path.is_file()}
    recovery = prepare_failed_run_reuse(**kwargs)
    assert recovery["required_nodes"] == ["model"]
    assert recovery["reuse_nodes"] == ["shared", "model"]
    assert recovery["recompute_nodes"] == ["validity"]
    assert recovery["source_checkpoints_by_node"] == {
        node: expectations[node].node_execution_id for node in ("shared", "model")
    }
    for node in ("shared", "model"):
        CheckpointStore(kwargs["target_run_root"], create=False).verify(expectations[node])
    assert prepare_failed_run_reuse(**kwargs) == recovery
    assert {str(path.relative_to(source)): path.read_bytes() for path in source.rglob("*") if path.is_file()} == before
    assert not (kwargs["target_run_root"] / "events.jsonl").exists()


@pytest.mark.parametrize("change", [
    "unknown", "duplicate", "unsuccessful", "ancestor", "input", "build", "dependency",
    "checkpoint", "external", "seed", "clock", "chain", "implementation", "definition", "event",
])
def test_strict_failed_reuse_rejects_before_runtime(tmp_path: Path, change: str) -> None:
    kwargs, expectations, values = _strict_failed_source(tmp_path, wrong_commit_event=change == "event")
    service = kwargs["service"]
    dag = kwargs["dag"]
    if change == "unknown":
        kwargs["required_node_ids"] = ("missing",)
    elif change == "duplicate":
        kwargs["required_node_ids"] = ("model", "model")
    elif change == "unsuccessful":
        kwargs["required_node_ids"] = ("validity",)
    elif change == "ancestor":
        kwargs["dag"] = replace(dag, nodes=(replace(dag.nodes[0], configuration_hash="f" * 64), *dag.nodes[1:]))
    elif change == "input":
        kwargs["dag"] = replace(
            dag, nodes=(*dag.nodes, replace(dag.nodes[0], node_id="other")),
            edges=(replace(dag.edges[0], source_node="other"), dag.edges[1]),
        )
    elif change in {"build", "dependency"}:
        changes = {"build_artifact_digest": "f" * 64} if change == "build" else {"dependency_distribution_digests": {"numpy": "f" * 64}}
        service.audit_environment = replace(service.audit_environment, **changes)
    elif change == "checkpoint":
        (kwargs["source_run_root"] / "checkpoints" / expectations["model"].node_execution_id / "content.bin").write_bytes(b"broken")
    elif change == "external":
        commit = values["model"].values["out"].external_commit
        (kwargs["source_run_root"] / "external-artifacts" / "objects" / commit.semantic_hash / "value.txt").write_text("changed", encoding="utf-8")
    elif change == "seed":
        kwargs["root_seed"] += 1
    elif change == "clock":
        kwargs["fixed_clock"] = "2026-09-29T00:00:00+08:00"
    elif change == "chain":
        path = kwargs["source_run_root"] / "operator-dag-run.json"
        record = json.loads(path.read_text(encoding="utf-8"))
        record["event_chain_head"] = "f" * 64
        path.write_text(canonical_json(record), encoding="utf-8")
    elif change in {"implementation", "definition"}:
        definition = service._definitions[dag.nodes[0].implementation_id]
        spec = definition.operator_spec
        implementation = definition.implementation_ref
        spec_payload = spec.payload()
        if change == "implementation":
            spec_payload["code_hash"] = "f" * 64
            implementation = replace(implementation, code_hash="f" * 64)
        else:
            spec_payload["operator_version"] = "99.0.0"
        spec = type(spec).from_dict({**spec_payload, "spec_hash": typed_canonical_hash(spec_payload)})
        service._definitions[dag.nodes[0].implementation_id] = type(definition).build(
            operator_spec=spec, implementation_ref=implementation,
            runtime_adapter_ref=definition.runtime_adapter_ref,
            cache_profile_ref=definition.cache_profile_ref,
            cache_compatibility_mode=definition.cache_compatibility_mode,
            resource_hint_ref=definition.resource_hint_ref, partition_keys=definition.partition_keys,
        )
    with pytest.raises(RuntimeIntegrityError):
        prepare_failed_run_reuse(**kwargs)
    assert not (kwargs["target_run_root"] / "events.jsonl").exists()
    assert not (kwargs["target_run_root"] / "recovery-plan.json").exists()
