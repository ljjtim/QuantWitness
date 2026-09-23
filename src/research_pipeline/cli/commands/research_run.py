"""已准入计划到数据平面、统一 Runtime 与自包含 Result 的完整命令。"""

from __future__ import annotations

from argparse import Namespace
import json
import hashlib
import os
from pathlib import Path
from typing import Mapping

from research_pipeline.data_plane import PathRolePolicy
from research_pipeline.data_plane.research_data_bundle import (
    merge_research_data_bundles,
)
from research_pipeline.extensions import (
    AdmittedProjectOperatorRegistry,
)
from research_pipeline.platform import canonical_json, installed_distribution_digest, typed_canonical_hash
from research_pipeline.platform.metric_contracts import (
    MetricReachabilityProof,
)
from research_pipeline.research.semantics import ResearchSemantics
from research_pipeline.results import ResultAssembler, ResultSpec
from research_pipeline.evidence.validity_recompute import (
    VALIDITY_FACTS_PRODUCER_HASH,
    policy_id_for_claim,
)
from research_pipeline.runtime import (
    AuditEnvironmentManifest,
    ArtifactRef,
    ExecutionMode,
    ExternalArtifactStore,
    ResourceCapacity,
    RuntimeExecutionService,
    ResourceGovernor,
    ResourceGovernorConfig,
    ResourceVector,
)
from research_pipeline.runtime.scheduler import (
    DEFAULT_RUNTIME_MEMORY_BYTES,
    DEFAULT_RUNTIME_SCRATCH_BYTES,
    available_cpu_slots,
)
from research_pipeline.runtime.diagnostics import (
    safe_error_summary,
    write_finalize_status,
)
from research_pipeline.runtime.operator_definitions import build_mainline_operator_manifest
from research_pipeline.runtime.adapters.common import ResearchRunEnvironment
from research_pipeline.runtime.operator_registry import (
    admitted_implementation_manifest_hash,
)
from research_pipeline.runtime.operator_graph_admission import (
    load_study_reproduction_proof,
)

from ..research_plan_store import load_operator_graph_research_plan
from ..result import execute_guarded


def execute(args) -> int:
    return execute_guarded(args, _execute)


def _execute(args) -> dict[str, object]:
    return _execute_operator_graph(args)


def _execute_operator_graph(args) -> dict[str, object]:
    if getattr(args, "execution_engine", "unified") != "unified":
        raise ValueError("execution_engine 迁移参数已删除；正式运行只使用统一 Runtime")
    _require_data_run_arguments(args)
    _validate_run_paths(args)
    manifest, admitted, dag, registry = load_operator_graph_research_plan(
        target=args.plan,
    )
    implementation_manifest_hash = admitted_implementation_manifest_hash(registry)
    if args.root_seed != manifest["root_seed"] or args.clock != manifest["fixed_clock"]:
        raise ValueError("run 的 clock/root_seed 必须与算子图计划完全一致")
    resource_capacity = _runtime_resource_capacity(args)
    args.workers = _resolve_worker_count(
        requested=getattr(args, "workers", None),
        mode=args.mode,
        dag=dag,
        capacity=resource_capacity,
        registry=registry,
    )
    _validate_resource_governance_arguments(args)
    database = Path(args.data_db).resolve()
    if not database.is_file():
        raise ValueError("显式只读 data-db 不存在")
    source_databases = _source_database_arguments(args)
    database_probe = (database.stat().st_size, database.stat().st_mtime_ns)
    source_database_probes = {
        profile: (path.stat().st_size, path.stat().st_mtime_ns)
        for profile, path in sorted(source_databases.items())
    }
    run_root = Path(args.run_root).resolve()
    run_root.mkdir(parents=True, exist_ok=True)
    _write_json_atomic(
        run_root / "research.identity.json",
        _mapping(manifest["research_identity"], "research_identity"),
    )
    _write_json_atomic(
        run_root / "research.metric-contract.json",
        _mapping(manifest["metric_contract"], "metric_contract"),
    )
    _write_json_atomic(
        run_root / "research.claim-contract.json",
        _mapping(manifest["claim_contract"], "claim_contract"),
    )
    if isinstance(manifest.get("research_semantics"), Mapping):
        _write_json_atomic(
            run_root / "research.semantics.json",
            {
                **dict(manifest["research_semantics"]),
                "effective_claim_level": manifest["effective_claim_level"],
            },
        )
    if args.root_seed != manifest["root_seed"] or args.clock != manifest["fixed_clock"]:
        raise ValueError("run 的 clock/root_seed 必须与算子图计划完全一致")
    study_proof = None
    if args.acceptance_proof:
        study_proof = load_study_reproduction_proof(
            args.acceptance_proof,
            expected_study_id=str(manifest["research_id"]),
            expected_package_hash=str(manifest["package_hash"]),
            expected_package_plan_hash=str(manifest["package_plan_hash"]),
        )
    runtime_result = _execute_unified_operator_runtime(
        args=args,
        manifest=manifest,
        admitted=admitted,
        dag=dag,
        registry=registry,
        database=database,
        source_databases=source_databases,
        minute_data_root=(
            None
            if not getattr(args, "minute_data_root", None)
            else Path(args.minute_data_root).resolve()
        ),
        resource_capacity=resource_capacity,
        study_reproduction_proof=study_proof,
    )
    result_payload = _finalize_operator_graph_result(
        args=args,
        manifest=manifest,
        admitted=admitted,
        runtime_result=runtime_result,
        implementation_manifest_hash=implementation_manifest_hash,
        database=database,
        database_probe=database_probe,
        source_databases=source_databases,
        source_database_probes=source_database_probes,
        study_proof=study_proof,
    )
    completion = {
        **result_payload,
        "next_action": "运行 verify，以自包含 Result 生成结构化 VerificationResult；通过后可使用 report、compare、export-result 或 Dashboard 消费结果。",
    }
    return completion


def _finalize_operator_graph_result(
    *,
    args,
    manifest: Mapping[str, object],
    admitted: Mapping[str, object],
    runtime_result: Mapping[str, object],
    implementation_manifest_hash: str,
    database: Path,
    database_probe: tuple[int, int],
    source_databases: Mapping[str, Path],
    source_database_probes: Mapping[str, tuple[int, int]],
    study_proof,
) -> dict[str, object]:
    """投影 Runtime 之后的唯一 Result finalize 生命周期。"""

    write_finalize_status(args.run_root, status="pending")
    result_bundle = None
    result_directory = None

    def capture_published_result(bundle, directory: Path) -> None:
        nonlocal result_bundle, result_directory
        result_bundle = bundle
        result_directory = directory

    try:
        completion_metadata = runtime_result.get("completion_metadata")
        if not isinstance(completion_metadata, Mapping):
            raise ValueError("统一 Runtime 缺少 completion metadata")
        proof_hashes = completion_metadata.get("proof_hashes")
        if study_proof is not None and (
            not isinstance(proof_hashes, Mapping)
            or proof_hashes.get("study_reproduction") != study_proof.proof_hash
        ):
            raise ValueError("StudyReproductionProof 未由对应领域 adapter 验证")
        if (database.stat().st_size, database.stat().st_mtime_ns) != database_probe:
            raise ValueError("算子图正式运行后数据库指纹发生变化")
        if {
            profile: (path.stat().st_size, path.stat().st_mtime_ns)
            for profile, path in sorted(source_databases.items())
        } != source_database_probes:
            raise ValueError("算子图正式运行后额外只读数据库指纹发生变化")
        data_bundle = _runtime_data_bundle(runtime_result, args.run_root)
        references = data_bundle.get("references")
        if not isinstance(references, Mapping):
            raise ValueError("正式 Result 缺少 data bundle references")
        claim_contract = manifest.get("claim_contract")
        if not isinstance(claim_contract, Mapping):
            raise ValueError("正式 Result 缺少 claim contract")
        raw_result_spec = manifest.get("result_spec")
        raw_metric_proofs = manifest.get("metric_proofs")
        if not isinstance(raw_result_spec, Mapping) or not isinstance(
            raw_metric_proofs, list
        ):
            raise ValueError("正式 run 缺少已编译 ResultSpec/metric proofs")
        result_spec = ResultSpec.from_dict(raw_result_spec)
        metric_proofs = tuple(
            MetricReachabilityProof.from_dict(item)
            for item in raw_metric_proofs
            if isinstance(item, Mapping)
        )
        if len(metric_proofs) != len(raw_metric_proofs):
            raise ValueError("正式 run metric proofs schema 无效")
        effective_claim_level = str(
            manifest.get("effective_claim_level", claim_contract["max_claim_level"])
        )
        result_bundle, result_directory, _ = ResultAssembler(
            args.result_store
        ).finalize(
            run_root=args.run_root,
            project_id=str(manifest["package_id"]),
            package_hash=str(manifest["package_hash"]),
            plan_hash=str(manifest["package_plan_hash"]),
            result_spec=result_spec,
            catalog_hashes={
                request_id: plan.catalog_hash
                for request_id, plan in sorted(admitted.items())
            },
            data_references=references,
            metric_proofs=metric_proofs,
            implementation_manifest_hash=implementation_manifest_hash,
            verification_policy_id=policy_id_for_claim(effective_claim_level),
            validity_producer_hash=VALIDITY_FACTS_PRODUCER_HASH,
            verifier_identity=manifest.get("verifier_admission"),
            formal_input_request_ids=tuple(manifest["consumed_request_ids"]),
            input_claim_ceilings=manifest["input_claim_ceilings"],
            published_hook=capture_published_result,
        )
        result_payload = {
            "contract_version": "research-run-completion-v1",
            "status": "result_finalized",
            "scope": "local_only",
            "package_id": manifest["package_id"],
            "package_plan_hash": manifest["package_plan_hash"],
            "dag_hash": manifest["dag_hash"],
            "study_reproduction_proof_hash": (
                None if study_proof is None else study_proof.proof_hash
            ),
            "database_unchanged": True,
            "execution_engine": "unified",
            "runtime_run_id": runtime_result["run_id"],
            "result_id": result_bundle.result_id,
            "result_directory": str(result_directory),
            "verification_policy_id": result_bundle.verification.policy_id,
            "completion_metadata": dict(completion_metadata),
        }
        write_finalize_status(
            args.run_root,
            status="succeeded",
            result_id=result_bundle.result_id,
            result_directory=str(result_directory),
            result_published=True,
        )
    except BaseException as exc:
        try:
            write_finalize_status(
                args.run_root,
                status="failed",
                result_id=(
                    None if result_bundle is None else result_bundle.result_id
                ),
                result_directory=(
                    None if result_directory is None else str(result_directory)
                ),
                result_published=result_bundle is not None,
                error=safe_error_summary(
                    exc,
                    default_error_code="result_finalize_failed",
                ),
            )
        except Exception:
            # 状态投影写入失败不能掩盖真实 finalize 异常。
            pass
        raise
    return result_payload


def _execute_unified_operator_runtime(
    *,
    args,
    manifest,
    admitted,
    dag,
    registry,
    database,
    source_databases,
    minute_data_root,
    resource_capacity,
    study_reproduction_proof,
):
    """把现行算子实现交给唯一统一 Runtime。"""
    runtime_root = Path(args.run_root).resolve()
    _write_or_verify_runtime_invocation(runtime_root, args)
    captured: dict[str, Mapping[str, object]] = {}
    raw_graph_plan = json.loads(
        (Path(args.plan).resolve() / "operator-graph-plan.json").read_text(
            encoding="utf-8"
        )
    )
    raw_nodes = raw_graph_plan.get("recipe", {}).get("nodes", [])
    if not isinstance(raw_nodes, list):
        raise ValueError("统一 Runtime operator graph nodes 无效")
    node_parameters = {
        str(item["node_id"]): item.get("parameters", {})
        for item in raw_nodes
        if isinstance(item, Mapping) and isinstance(item.get("parameters"), Mapping)
    }
    raw_semantics = manifest.get("research_semantics")
    semantics = (
        ResearchSemantics.from_dict(raw_semantics)
        if isinstance(raw_semantics, Mapping)
        else None
    )
    strategy_spec_hashes = {
        item.strategy_id: item.spec_hash for item in registry.strategy_specs
    }
    if len(strategy_spec_hashes) != len(registry.strategy_specs):
        raise ValueError("统一 Runtime StrategySpec ID 重复")
    operator_graph_strategy_hash = typed_canonical_hash({
        "contract_version": "operator-graph-strategy-v1",
        "nodes": raw_nodes,
    })

    environment = ResearchRunEnvironment(
        plan_root=Path(args.plan).resolve(),
        manifest=manifest,
        admitted_plans=admitted,
        database=database,
        source_databases=source_databases,
        minute_data_root=minute_data_root,
        holdout_ledger_anchor=_holdout_ledger_anchor(args),
        node_parameters=node_parameters,
        semantics=semantics,
        fixed_clock=args.clock,
        root_seed=args.root_seed,
        workers=args.workers,
        resource_timeout_seconds=getattr(args, "resource_timeout_seconds", None),
        strategy_spec_hashes=strategy_spec_hashes,
        operator_graph_strategy_hash=operator_graph_strategy_hash,
        study_reproduction_proof=study_reproduction_proof,
        captured=captured,
    )

    audit, dependencies = _build_runtime_audit_environment()
    resource_governor = _build_resource_governor(args, resource_capacity)
    service = RuntimeExecutionService(
        audit_environment=audit,
        numerical_backend_names=tuple(sorted(dependencies)),
        resource_capacity=resource_capacity,
        resource_governor=resource_governor,
        resource_timeout_seconds=getattr(args, "resource_timeout_seconds", None),
        project_registry=(
            registry if isinstance(registry, AdmittedProjectOperatorRegistry) else None
        ),
        project_parameters_by_node=node_parameters,
    )
    process_slots_by_node = {node.node_id: 1 for node in dag.nodes}
    runtime_kwargs = {
        "dag": dag,
        "environment": environment,
        "run_root": args.run_root,
        "project_id": str(manifest["package_id"]),
        "root_seed": args.root_seed,
        "fixed_clock": args.clock,
        "mode": ExecutionMode(args.mode),
        "process_slots_by_node": process_slots_by_node,
    }
    recovery = None
    parent_run_root = getattr(args, "runtime_rerun_parent_root", None)
    if parent_run_root:
        recovery_path = runtime_root / "recovery-plan.json"
        if recovery_path.is_file():
            recovery = json.loads(recovery_path.read_text(encoding="utf-8"))
        else:
            recovery = service.prepare_rerun_from(
                dag=dag,
                parent_run_root=parent_run_root,
                child_run_root=runtime_root,
                project_id=str(manifest["package_id"]),
                root_seed=args.root_seed,
                fixed_clock=args.clock,
                mode=ExecutionMode(args.mode),
                node_id=str(args.runtime_rerun_node),
            )
        runtime_kwargs.update({
            "parent_run_id": recovery["parent_run_id"],
            "rerun_from_node": recovery["rerun_from_node"],
            "recovery_plan_hash": recovery["recovery_plan_hash"],
        })
    retry_node_id = getattr(args, "runtime_retry_node", None)
    if retry_node_id:
        return service.retry_node(**runtime_kwargs, node_id=retry_node_id)
    return service.execute(
        **runtime_kwargs,
        resume=bool(getattr(args, "runtime_resume", False)),
    )


def _build_runtime_audit_environment(
) -> tuple[AuditEnvironmentManifest, dict[str, str]]:
    dependencies = {}
    for name in ("numpy", "pandas", "pyarrow", "duckdb"):
        distribution_name, _, digest = installed_distribution_digest(name)
        dependencies[distribution_name.lower()] = digest
    operator_manifest = build_mainline_operator_manifest()
    build_digest = typed_canonical_hash(
        {
            "operator_manifest_hash": operator_manifest.manifest_hash,
            "runtime_execution_service": hashlib.sha256(
                (
                    Path(__file__).resolve().parents[2]
                    / "runtime"
                    / "execution_service.py"
                ).read_bytes()
            ).hexdigest(),
            "resource_governor": hashlib.sha256(
                (
                    Path(__file__).resolve().parents[2]
                    / "runtime"
                    / "resource_governor.py"
                ).read_bytes()
            ).hexdigest(),
        }
    )
    return (
        AuditEnvironmentManifest.capture(
            build_artifact_digest=build_digest,
            dependency_distribution_digests=dependencies,
        ),
        dependencies,
    )


def _write_or_verify_runtime_invocation(runtime_root: Path, args) -> None:
    """保存正式恢复所需的不可变调用参数，不把数据库内容写入运行目录。"""
    holdout_ledger_anchor = _holdout_ledger_anchor(args)
    runtime_root.mkdir(parents=True, exist_ok=True)
    target = runtime_root / "operator-dag-invocation.json"
    source_databases = _source_database_arguments(args)
    rerun_enabled = bool(getattr(args, "runtime_rerun_parent_root", None))
    resource_capacity = _runtime_resource_capacity(args)
    payload = {
        "contract_version": "research-operator-dag-invocation-v10",
        "plan": str(Path(args.plan).resolve()),
        "data_db": str(Path(args.data_db).resolve()),
        "source_dbs": {
            profile: str(path) for profile, path in sorted(source_databases.items())
        },
        "minute_data_root": (
            None
            if not getattr(args, "minute_data_root", None)
            else str(Path(args.minute_data_root).resolve())
        ),
        "artifact_root": str(Path(args.artifact_root).resolve()),
        "holdout_ledger_anchor": str(holdout_ledger_anchor),
        "handoff_out": str(Path(args.handoff_out).resolve()),
        "run_root": str(runtime_root),
        "result_store": str(Path(args.result_store).resolve()),
        "mode": args.mode,
        "workers": args.workers,
        "root_seed": args.root_seed,
        "clock": args.clock,
        "acceptance_proof": (
            None if not args.acceptance_proof else str(Path(args.acceptance_proof).resolve())
        ),
        "execution_engine": "unified",
        "resource_capacity": resource_capacity.to_dict(),
    }
    if rerun_enabled:
        payload["parent_run_root"] = str(Path(args.runtime_rerun_parent_root).resolve())
        payload["rerun_from_node"] = str(args.runtime_rerun_node)
    payload["resource_governance"] = (
        None
        if not getattr(args, "resource_state_dir", None)
        else {
            "state_dir": str(Path(args.resource_state_dir).resolve()),
            "process_slots": args.resource_process_slots,
            "timeout_seconds": args.resource_timeout_seconds,
            "stale_seconds": args.resource_stale_seconds,
        }
    )
    document = {**payload, "invocation_hash": typed_canonical_hash(payload)}
    if target.exists():
        existing = json.loads(target.read_text(encoding="utf-8"))
        if existing != document:
            raise ValueError("正式 operator DAG 恢复参数与首次调用不一致")
        return
    _write_json_atomic(target, document)


def _load_operator_invocation(run_root: str | Path) -> Namespace:
    root = Path(run_root).resolve()
    target = root / "operator-dag-invocation.json"
    payload = json.loads(target.read_text(encoding="utf-8"))
    invocation_hash = payload.pop("invocation_hash", None)
    original_payload = dict(payload)
    version = payload.get("contract_version")
    expected = {
        "contract_version", "plan", "data_db", "source_dbs", "minute_data_root", "artifact_root", "holdout_ledger_anchor", "handoff_out",
        "run_root", "result_store", "mode", "workers", "root_seed", "clock",
        "acceptance_proof", "execution_engine", "resource_capacity",
        "resource_governance",
    }
    if "parent_run_root" in payload or "rerun_from_node" in payload:
        expected.update({"parent_run_root", "rerun_from_node"})
    if (
        set(payload) != expected
        or version != "research-operator-dag-invocation-v10"
        or payload.get("run_root") != str(root)
        or payload.get("execution_engine") != "unified"
        or invocation_hash != typed_canonical_hash(original_payload)
    ):
        raise ValueError("正式 operator DAG invocation 版本、路径或 hash 无效")
    source_dbs = payload.pop("source_dbs")
    payload.setdefault("minute_data_root", None)
    if not isinstance(source_dbs, Mapping):
        raise ValueError("正式 operator DAG source_dbs 无效")
    capacity_payload = payload.pop("resource_capacity", None)
    if not isinstance(capacity_payload, Mapping) or set(capacity_payload) != {
        "memory_bytes", "cpu_slots", "temp_bytes", "max_workers"
    }:
        raise ValueError("正式 operator DAG resource_capacity 无效")
    capacity = ResourceCapacity(**capacity_payload)
    governance = payload.pop("resource_governance", None)
    if governance is not None and (
        not isinstance(governance, Mapping)
        or set(governance) != {
            "state_dir", "process_slots", "timeout_seconds", "stale_seconds"
        }
    ):
        raise ValueError("正式 operator DAG resource_governance 无效")
    parent_run_root = payload.pop("parent_run_root", None)
    rerun_from_node = payload.pop("rerun_from_node", None)
    return Namespace(
        **{key: value for key, value in payload.items() if key != "contract_version"},
        source_db=[f"{profile}={path}" for profile, path in sorted(source_dbs.items())],
        json=True,
        runtime_resume=True,
        runtime_retry_node=None,
        runtime_rerun_parent_root=parent_run_root,
        runtime_rerun_node=rerun_from_node,
        resource_state_dir=None if governance is None else governance["state_dir"],
        resource_memory_bytes=capacity.memory_bytes,
        resource_cpu_slots=capacity.cpu_slots,
        resource_scratch_bytes=capacity.temp_bytes,
        resource_process_slots=None if governance is None else governance["process_slots"],
        resource_timeout_seconds=None if governance is None else governance["timeout_seconds"],
        resource_stale_seconds=30.0 if governance is None else governance["stale_seconds"],
    )


def inspect_operator_graph(run_root: str | Path) -> dict[str, object]:
    """只读取计划闭包和 run-root，按正式身份复验 checkpoint。"""

    args = _load_operator_invocation(run_root)
    _manifest, _admitted, dag, registry = load_operator_graph_research_plan(
        target=args.plan
    )
    audit, dependencies = _build_runtime_audit_environment()
    service = RuntimeExecutionService(
        audit_environment=audit,
        numerical_backend_names=tuple(sorted(dependencies)),
        project_registry=(
            registry if isinstance(registry, AdmittedProjectOperatorRegistry) else None
        ),
    )
    return {
        "dag": dag,
        "checkpoints": service.inspect_checkpoints(
            dag=dag,
            run_root=run_root,
            root_seed=args.root_seed,
            fixed_clock=args.clock,
        ),
    }


def resume_operator_graph(*, run_root: str | Path, retry_node_id: str | None = None) -> dict[str, object]:
    """由公开 resume/retry-node 命令恢复正式 operator DAG。"""
    args = _load_operator_invocation(run_root)
    args.runtime_retry_node = retry_node_id
    return _execute_operator_graph(args)


def rerun_operator_graph(
    *, parent_run_root: str | Path, child_run_root: str | Path, node_id: str
) -> dict[str, object]:
    """从完整正式父运行创建独立 child invocation 并使用同一 Runtime。"""
    parent = Path(parent_run_root).resolve()
    child = Path(child_run_root).resolve()
    if child.exists():
        raise ValueError("rerun-from output-run-root 必须不存在")
    args = _load_operator_invocation(parent)
    args.run_root = str(child)
    args.artifact_root = str(child.with_name(f"{child.name}-artifacts"))
    args.handoff_out = str(child.with_name(f"{child.name}-handoff.json"))
    args.result_store = str(child.with_name(f"{child.name}-results"))
    args.runtime_resume = True
    args.runtime_retry_node = None
    args.runtime_rerun_parent_root = str(parent)
    args.runtime_rerun_node = node_id
    _validate_run_paths(args)
    return _execute_operator_graph(args)


def _holdout_ledger_anchor(args) -> Path:
    """验证 rerun 血缘，并让所有后代继承根运行的 holdout 生命周期。"""

    current = args
    seen_run_roots: set[Path] = set()
    declared_anchor: Path | None = None
    while True:
        current_run_root = Path(current.run_root).resolve()
        if current_run_root in seen_run_roots:
            raise ValueError("rerun lineage 存在循环")
        seen_run_roots.add(current_run_root)

        raw_anchor = getattr(current, "holdout_ledger_anchor", None)
        if raw_anchor is not None:
            current_anchor = Path(raw_anchor).resolve()
            if declared_anchor is None:
                declared_anchor = current_anchor
            elif current_anchor != declared_anchor:
                raise ValueError("rerun lineage 的 holdout ledger anchor 冲突")

        parent_run_root = getattr(current, "runtime_rerun_parent_root", None)
        if not parent_run_root:
            root_anchor = Path(current.artifact_root).resolve()
            if declared_anchor is not None and declared_anchor != root_anchor:
                raise ValueError("rerun lineage 的根 holdout ledger anchor 冲突")
            return root_anchor

        parent = Path(parent_run_root).resolve()
        if parent in seen_run_roots:
            raise ValueError("rerun lineage 存在循环")
        try:
            current = _load_operator_invocation(parent)
        except (OSError, ValueError) as exc:
            raise ValueError(f"rerun lineage 无法加载父运行: {parent}") from exc


def _runtime_data_bundle(
    runtime_result: Mapping[str, object], run_root: str | Path
) -> Mapping[str, object]:
    """从本次 Runtime 已提交的数据工件闭合 Result 输入引用。"""

    raw_outputs = runtime_result.get("outputs")
    if not isinstance(raw_outputs, Mapping):
        raise ValueError("统一 Runtime 缺少节点输出索引")
    store = ExternalArtifactStore(
        Path(run_root).resolve() / "external-artifacts", create=False
    )
    bundles = []
    data_artifact_types = {
        "data.columnar-bundle.v1",
        "data.minute-bars.1m.v1",
    }
    for node_id, raw_node_outputs in sorted(raw_outputs.items()):
        if not isinstance(raw_node_outputs, Mapping):
            raise ValueError(f"统一 Runtime 节点输出索引无效: {node_id}")
        for port, raw_reference in sorted(raw_node_outputs.items()):
            if not isinstance(raw_reference, Mapping):
                raise ValueError(f"统一 Runtime 输出引用无效: {node_id}/{port}")
            reference = ArtifactRef.from_dict(dict(raw_reference))
            if reference.artifact_type not in data_artifact_types:
                continue
            commit = store.verify(reference.artifact_key)
            if commit.artifact_ref != reference:
                raise ValueError(f"统一 Runtime 数据输出引用漂移: {node_id}/{port}")
            payload_path = store.objects_root / commit.semantic_hash / "result.json"
            try:
                payload = json.loads(payload_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise ValueError(
                    f"统一 Runtime 数据输出不可读: {node_id}/{port}"
                ) from exc
            bundle = payload.get("data_bundle") if isinstance(payload, Mapping) else None
            if not isinstance(bundle, Mapping):
                raise ValueError(f"统一 Runtime 数据输出缺少 data_bundle: {node_id}/{port}")
            bundles.append(bundle)
    return merge_research_data_bundles(bundles)


def _write_json_atomic(path: Path, payload: Mapping[str, object]) -> None:
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    temporary.write_text(canonical_json(dict(payload)), encoding="utf-8")
    os.replace(temporary, path)


def _validate_run_paths(args) -> None:
    roles = {
        "plan_input": args.plan,
        "database_input": args.data_db,
        "artifact_output": args.artifact_root,
        "handoff_output": args.handoff_out,
        "run_output": args.run_root,
        "result_output": args.result_store,
    }
    if getattr(args, "minute_data_root", None):
        roles["minute_data_input"] = args.minute_data_root
    if args.acceptance_proof:
        roles["acceptance_proof_input"] = args.acceptance_proof
    if getattr(args, "resource_state_dir", None):
        roles["resource_governance_output"] = args.resource_state_dir
    read_only = ["plan_input", "database_input"]
    if getattr(args, "minute_data_root", None):
        read_only.append("minute_data_input")
    for profile, path in sorted(_source_database_arguments(args).items()):
        role = f"source_database_input_{profile}"
        roles[role] = str(path)
        read_only.append(role)
    if args.acceptance_proof:
        read_only.append("acceptance_proof_input")
    PathRolePolicy().validate(roles, read_only_roles=tuple(read_only))


def _require_data_run_arguments(args) -> None:
    missing = [
        flag
        for flag, attribute in (
            ("--data-db", "data_db"),
            ("--handoff-out", "handoff_out"),
            ("--result-store", "result_store"),
        )
        if not getattr(args, attribute, None)
    ]
    if missing:
        raise ValueError(f"正式数据研究缺少必需参数: {', '.join(missing)}")


def _validate_resource_governance_arguments(args) -> ResourceCapacity:
    capacity = _runtime_resource_capacity(args)
    state_dir = getattr(args, "resource_state_dir", None)
    if state_dir is None:
        if any(
            getattr(args, name, None) is not None
            for name in ("resource_process_slots", "resource_timeout_seconds")
        ):
            raise ValueError(
                "resource-process-slots/timeout 只能与 --resource-state-dir 一起提供"
            )
        return capacity
    shared_values = (
        getattr(args, "resource_process_slots", None),
        getattr(args, "resource_timeout_seconds", None),
    )
    if any(
        value is None
        or isinstance(value, bool)
        or not isinstance(value, (int, float))
        or value <= 0
        for value in shared_values
    ):
        raise ValueError(
            "显式跨进程治理必须提供正数 process-slots 和 timeout"
        )
    if getattr(args, "resource_stale_seconds", 0) <= 0:
        raise ValueError("resource-stale-seconds 必须为正数")
    requested_workers = getattr(args, "workers", None)
    required_process_slots = (
        1
        if requested_workers in (None, 1)
        else int(requested_workers) + 1
    )
    if int(args.resource_process_slots) < required_process_slots:
        raise ValueError("resource-process-slots 小于本次 CLI 与内部 worker 所需槽位")
    return capacity


def _build_resource_governor(
    args,
    capacity: ResourceCapacity,
) -> ResourceGovernor | None:
    if not getattr(args, "resource_state_dir", None):
        return None
    config = ResourceGovernorConfig(
        Path(args.resource_state_dir),
        ResourceVector(
            capacity.memory_bytes,
            capacity.cpu_slots,
            capacity.temp_bytes,
            args.resource_process_slots,
        ),
        stale_after_seconds=args.resource_stale_seconds,
    )
    return ResourceGovernor(config)


def _runtime_resource_capacity(args) -> ResourceCapacity:
    memory_bytes = getattr(args, "resource_memory_bytes", None)
    cpu_slots = getattr(args, "resource_cpu_slots", None)
    scratch_bytes = getattr(args, "resource_scratch_bytes", None)
    memory_bytes = (
        DEFAULT_RUNTIME_MEMORY_BYTES if memory_bytes is None else memory_bytes
    )
    cpu_slots = available_cpu_slots() if cpu_slots is None else cpu_slots
    scratch_bytes = (
        DEFAULT_RUNTIME_SCRATCH_BYTES if scratch_bytes is None else scratch_bytes
    )
    return ResourceCapacity(
        memory_bytes=memory_bytes,
        cpu_slots=cpu_slots,
        temp_bytes=scratch_bytes,
        max_workers=cpu_slots,
    )


def _resolve_worker_count(
    *,
    requested: int | None,
    mode: str,
    dag,
    capacity: ResourceCapacity,
    registry=None,
) -> int:
    if requested is not None and (type(requested) is not int or requested < 1):
        raise ValueError("workers 必须是正整数")
    ExecutionMode(mode)

    worker_limit = min(capacity.cpu_slots, capacity.max_workers)
    if requested is not None and requested > worker_limit:
        raise ValueError("workers 超过本次 CPU 或 max_workers 容量")
    return 1 if requested is None else requested


def _source_database_arguments(args) -> dict[str, Path]:
    raw_values = getattr(args, "source_db", ()) or ()
    if not isinstance(raw_values, (list, tuple)):
        raise ValueError("source-db 参数必须可重复传入")
    result: dict[str, Path] = {}
    for raw in raw_values:
        if not isinstance(raw, str) or "=" not in raw:
            raise ValueError("source-db 必须使用 PROFILE=PATH")
        profile, path_text = raw.split("=", 1)
        profile = profile.strip()
        if not profile or profile == "source" or profile in result:
            raise ValueError("source-db profile 必须非空、唯一且不能覆盖 source")
        path = Path(path_text).resolve()
        if not path.is_file():
            raise ValueError(f"source-db 不存在: {profile}")
        result[profile] = path
    return result


def _mapping(value: object, field: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{field} 必须是对象")
    return value


__all__ = ["execute", "resume_operator_graph"]
