from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from research_pipeline.platform import canonical_json, typed_canonical_hash
from research_pipeline.platform.operator_contracts import operator_dag_runtime_hash
from research_pipeline.extensions import (
    OperatorSpec,
    PortSpec,
    build_admitted_operator_registry,
    compile_project_operator_bundle,
    compile_project_verifier_bundle,
    project_source_hash,
    verify_project_verifier_bundle,
)
from research_pipeline.extensions.verifier_bundle import ProjectVerifierBundleManifest
from research_pipeline.extensions.errors import ExtensionError
from research_pipeline.platform.metric_contracts import (
    MetricDefinition,
    MetricReachabilityProof,
    build_mainline_metric_registry,
)
from research_pipeline.results import (
    BAR_TCA_SCHEMA_IDS,
    CANONICAL_SIMULATION_SCHEMA_IDS,
    ResultAssembler,
    ResultContractError,
    ResultSnapshot,
    ResultSpec,
    ResultStore,
    ResultSupportFile,
    ResultTableSpec,
    metrics_from_snapshot,
)
from research_pipeline.evidence.validity_recompute import (
    VALIDITY_FACTS_PRODUCER_HASH,
    policy_id_for_claim,
)
from research_pipeline.evidence import (
    compare_verification_results,
    load_verified_result_context,
    verify_result,
)
from research_pipeline.evidence.errors import EvidenceContractError
from research_pipeline.evidence.verification_result import _verify_input_claim_lineage
from research_pipeline.cli.research_plan_store import (
    _copy_verifier_bundle_closure,
    _verify_plan_verifier_bundle,
)
from research_pipeline.runtime import EventStore
from research_pipeline.runtime.contracts import ResourceBudget
from research_pipeline.runtime.external_artifact import ExternalArtifactStore
from research_pipeline.runtime.operator_registry import build_mainline_operator_registry
from research_pipeline.runtime.project_operator_runtime import (
    ProjectRuntimeInput,
    execute_project_worker_attempt,
)
import research_pipeline.results.store as result_store_module
from validity_facts_support import default_passing_validity_facts


HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64
HASH_D = "d" * 64


def _write_succeeded_events(
    run_root: Path,
    *,
    run_id: str,
    node_ids: tuple[str, ...],
) -> str:
    store = EventStore(run_root)
    for index, status in enumerate(("created", "planned", "running"), 1):
        store.append(
            run_id,
            "run_status_changed",
            {"status": status},
            command_id=f"run-{index}",
        )
    for node_id in node_ids:
        for index, status in enumerate(("pending", "ready", "running", "succeeded"), 1):
            store.append(
                run_id,
                "node_status_changed",
                {"status": status},
                command_id=f"{node_id}-{index}",
                node_id=node_id,
            )
    return store.append(
        run_id,
        "run_status_changed",
        {"status": "succeeded"},
        command_id="run-succeeded",
    ).event_hash


def _commit_validity(
    external: ExternalArtifactStore,
    *,
    artifact_type: str = "research.validity-facts.v1",
    payload: dict[str, object] | None = None,
):
    staging = external.prepare()
    (staging / "result.json").write_text(
        canonical_json(
            default_passing_validity_facts() if payload is None else payload
        ),
        encoding="utf-8",
    )
    return external.commit(
        staging,
        artifact_name="validity",
        artifact_type=artifact_type,
    )


def _create_directory_link(link: Path, target: Path) -> None:
    if os.name == "nt":
        escaped_link = str(link).replace("'", "''")
        escaped_target = str(target).replace("'", "''")
        result = subprocess.run(
            [
                "powershell.exe",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                (
                    f"New-Item -ItemType Junction -Path '{escaped_link}' "
                    f"-Target '{escaped_target}' | Out-Null"
                ),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            pytest.fail(
                f"Windows junction 创建失败，退出码 {result.returncode}: "
                f"{result.stdout.strip()} {result.stderr.strip()}"
            )
        return
    link.symlink_to(target, target_is_directory=True)


def _remove_directory_link(path: Path) -> None:
    if os.name == "nt":
        path.rmdir()
    else:
        path.unlink()


def _proof(definition: MetricDefinition | None = None) -> MetricReachabilityProof:
    definition = definition or build_mainline_metric_registry().require(
        "data.row_count@1.0.0"
    )
    metric_ref = definition.metric_ref
    payload = {
        "metric_ref": metric_ref,
        "definition_digest": definition.definition_digest,
        "producer_node_id": "statistics",
        "producer_port": "statistics",
        "artifact_type": "data.columnar-bundle.v1",
        "result_table_id": "primary_statistics",
        "result_schema_id": "data.columnar-bundle.metrics.v1",
        "result_path_prefix": "statistics",
        "contract_version": "research-metric-reachability-v2",
    }
    return MetricReachabilityProof(**payload, proof_digest=typed_canonical_hash(payload))


def _spec() -> ResultSpec:
    return ResultSpec.build((ResultTableSpec(
        table_id="primary_statistics",
        role="primary",
        source_node_id="statistics",
        source_port="statistics",
        artifact_type="data.columnar-bundle.v1",
        schema_id="data.columnar-bundle.metrics.v1",
        path_prefix="statistics",
    ),))


def _data_reference() -> dict[str, object]:
    return {
        "relative_path": "objects/a",
        "physical_snapshot_id": HASH_A,
        "manifest_hash": HASH_B,
        "schema_hash": HASH_C,
        "source_revision_hash": HASH_D,
        "partitions": ["date=2026-01-01/part-0.parquet"],
        "contract_version": "dataset-artifact-ref-v1",
    }


def _runtime_fixture(
    tmp_path: Path,
    *,
    validity_artifact_type: str = "research.validity-facts.v1",
    validity_payload: dict[str, object] | None = None,
    metric_ref: str = "data.row_count@1.0.0",
) -> tuple[Path, Path, Path]:
    run_root = tmp_path / "run"
    external = ExternalArtifactStore(run_root / "external-artifacts")
    staging = external.prepare()
    (staging / "statistics").mkdir()
    pq.write_table(
        pa.table({
            "metric_ref": [metric_ref],
            "value": [1.25],
            "unit": ["rows" if metric_ref == "data.row_count@1.0.0" else "decimal_return"],
            "sample_start": ["2026-01-01"],
            "sample_end": ["2026-06-30"],
            "sample_size": pa.array([120], type=pa.int64()),
            "status": ["computed"],
        }),
        staging / "statistics" / "metrics.parquet",
    )
    (staging / "result.json").write_text("{}", encoding="utf-8")
    commit = external.commit(
        staging,
        artifact_name="statistics",
        artifact_type="data.columnar-bundle.v1",
    )
    validity = _commit_validity(
        external,
        artifact_type=validity_artifact_type,
        payload=validity_payload,
    )
    node_ids = ("statistics", "validity")
    dag = {
        "contract_version": "test",
        "nodes": [{"node_id": node_id} for node_id in node_ids],
        "edges": [],
    }
    event_chain_head = _write_succeeded_events(
        run_root,
        run_id=HASH_A,
        node_ids=node_ids,
    )
    record = {
        "contract_version": "research-runtime-operator-dag-run-v3",
        "status": "succeeded",
        "project_id": "research_package_test",
        "run_id": HASH_A,
        "dag_id": typed_canonical_hash(dag),
        "dag": dag,
        "root_seed": 7,
        "fixed_clock": "2026-07-25T00:00:00+08:00",
        "mode": "deterministic_serial",
        "audit_environment": {"python": "test", "backend": "pyarrow"},
        "audit_manifest_digest": HASH_C,
        "reused_nodes": [],
        "outputs": {
            "statistics": {"statistics": commit.artifact_ref.to_dict()},
            "validity": {"validity": validity.artifact_ref.to_dict()},
        },
        "completion_metadata": {
            "artifact_hashes": {},
            "proof_hashes": {},
            "counts": {},
            "backend_id": None,
            "fidelity": None,
            "limitations": [],
            "contract_version": "research-runtime-completion-metadata-v1",
        },
        "event_chain_head": event_chain_head,
    }
    (run_root / "operator-dag-run.json").write_text(
        canonical_json(record), encoding="utf-8",
    )
    return run_root, tmp_path / "results", external.objects_root / commit.semantic_hash


def _dual_table_runtime_fixture(tmp_path: Path) -> tuple[Path, Path]:
    run_root = tmp_path / "run"
    external = ExternalArtifactStore(run_root / "external-artifacts")
    outputs = {}
    for node_id, port, artifact_type, prefix, table in (
        (
            "statistics",
            "statistics",
            "data.columnar-bundle.v1",
            "statistics",
            pa.table({"metric": ["alpha"], "value": [1.25]}),
        ),
        (
            "diagnostics",
            "diagnostics",
            "research.diagnostics.v1",
            "diagnostics",
            pa.table({"reason": ["sample_count"], "value": [32]}),
        ),
    ):
        staging = external.prepare()
        (staging / prefix).mkdir()
        pq.write_table(table, staging / prefix / "part-0.parquet")
        commit = external.commit(
            staging,
            artifact_name=port,
            artifact_type=artifact_type,
        )
        outputs[node_id] = {port: commit.artifact_ref.to_dict()}
    validity = _commit_validity(external)
    outputs["validity"] = {"validity": validity.artifact_ref.to_dict()}
    node_ids = tuple(outputs)
    dag = {
        "contract_version": "test",
        "nodes": [{"node_id": node_id} for node_id in node_ids],
        "edges": [],
    }
    event_chain_head = _write_succeeded_events(
        run_root,
        run_id=HASH_A,
        node_ids=node_ids,
    )
    record = {
        "contract_version": "research-runtime-operator-dag-run-v3",
        "status": "succeeded",
        "project_id": "research_package_test",
        "run_id": HASH_A,
        "dag_id": typed_canonical_hash(dag),
        "dag": dag,
        "root_seed": 7,
        "fixed_clock": "2026-07-25T00:00:00+08:00",
        "mode": "deterministic_serial",
        "audit_environment": {"python": "test", "backend": "pyarrow"},
        "audit_manifest_digest": HASH_C,
        "reused_nodes": [],
        "outputs": outputs,
        "completion_metadata": {
            "artifact_hashes": {},
            "proof_hashes": {},
            "counts": {},
            "backend_id": None,
            "fidelity": None,
            "limitations": [],
            "contract_version": "research-runtime-completion-metadata-v1",
        },
        "event_chain_head": event_chain_head,
    }
    (run_root / "operator-dag-run.json").write_text(
        canonical_json(record), encoding="utf-8",
    )
    return run_root, tmp_path / "results"


def _heterogeneous_project_context(
    tmp_path: Path,
    *,
    project_id: str,
    verifier_id: str,
    node_id: str,
    artifact_type: str,
    schema_id: str,
    definition: MetricDefinition,
    extra_column: tuple[str, pa.Array],
    include_sibling: bool,
):
    run_root = tmp_path / "run"
    external = ExternalArtifactStore(run_root / "external-artifacts")
    input_staging = external.prepare()
    pq.write_table(pa.table({"seed": [0.5]}), input_staging / "seed.parquet")
    input_commit = external.commit(
        input_staging, artifact_name="observations",
        artifact_type="data.columnar-bundle.v1",
    )
    verified_input = external.verify(input_commit.semantic_hash)
    source = tmp_path / "operator_source"
    source.mkdir(parents=True)
    column_name, column = extra_column
    extra_value = column.to_pylist()[0]
    value_expression = "seed * 2" if column_name == "alpha_group" else "seed / 2"
    (source / "operator.py").write_text(
        "import pyarrow as pa\n"
        "import pyarrow.parquet as pq\n\n"
        "def run(context, inputs, output_root):\n"
        "    rows = [row for batch in inputs[0].iter_batches(\n"
        "        columns=('seed',), batch_size=1024) for row in batch.to_pylist()]\n"
        "    if len(rows) != 1:\n"
        "        raise ValueError('输入行数不闭合')\n"
        "    seed = rows[0]['seed']\n"
        "    target = output_root / 'metrics'\n"
        "    target.mkdir()\n"
        "    pq.write_table(pa.table({\n"
        f"        'metric_ref': [{definition.metric_ref!r}],\n"
        f"        'value': [{value_expression}],\n"
        f"        'unit': [{definition.unit!r}],\n"
        "        'sample_start': ['2026-01-01'],\n"
        "        'sample_end': ['2026-01-31'],\n"
        "        'sample_size': pa.array([1], type=pa.int64()),\n"
        "        'status': ['computed'],\n"
        f"        {column_name!r}: [{extra_value!r}],\n"
        "    }), target / 'part-00000.parquet')\n"
        "    return output_root.commit_directory(\n"
        f"        port='result', artifact_type={artifact_type!r},\n"
        "        relative_path='metrics', files=('part-00000.parquet',))\n",
        encoding="utf-8",
    )
    budget = ResourceBudget(1024 * 1024 * 1024, 1, 64 * 1024 * 1024, 30)
    spec = OperatorSpec.build(
        operator_id=f"project.{project_id}.metric",
        operator_version="1.0.0",
        input_ports=(PortSpec("observations", "data.columnar-bundle.v1"),),
        output_ports=(PortSpec("result", artifact_type),),
        parameters=(), strategy_roles=(),
        resource_profile=budget.to_dict(),
        determinism_mode="deterministic", seed_policy="none",
        code_hash=project_source_hash(source),
    )
    builtin = build_mainline_operator_registry()
    operator_bundle = compile_project_operator_bundle(
        source_root=source, output_root=tmp_path / "operator_bundles",
        project_id=project_id, operator_spec=spec,
        entry_module="operator", entry_function="run",
        dependency_lock={"pyarrow": pa.__version__},
        registered_operator_specs=builtin.operator_specs,
        project_artifact_types=(artifact_type,),
    )
    registry = build_admitted_operator_registry(
        (operator_bundle,), builtin_registry=builtin,
    )
    attempt_root = tmp_path / "operator_attempt"
    attempt_root.mkdir()
    worker_result = execute_project_worker_attempt(
        registry=registry,
        implementation_id=next(iter(registry.implementation_identities)),
        node_id=node_id, run_id=HASH_A, attempt_id="attempt-1",
        attempt_root=attempt_root,
        inputs=(ProjectRuntimeInput.from_verified(
            verified_input.artifact_ref, b"", HASH_D,
            source_root=external.objects_root / verified_input.semantic_hash,
            files=verified_input.files,
        ),),
        parameters={}, fixed_clock="2026-01-31T16:00:00+08:00",
        root_seed=7, budget=budget,
    )
    assert len(worker_result.outputs) == 1
    assert worker_result.outputs[0].artifact_type == artifact_type
    staging = external.prepare()
    (staging / "metrics").mkdir()
    shutil.copyfile(
        worker_result.outputs[0].path / "part-00000.parquet",
        staging / "metrics" / "part-00000.parquet",
    )
    (staging / "result.json").write_text("{}", encoding="utf-8")
    commit = external.commit(
        staging,
        artifact_name="result",
        artifact_type=artifact_type,
    )
    outputs = {
        "data_input": {"observations": verified_input.artifact_ref.to_dict()},
        node_id: {"result": commit.artifact_ref.to_dict()},
    }
    if include_sibling:
        sibling_staging = external.prepare()
        (sibling_staging / "unrelated").mkdir()
        pq.write_table(
            pa.table({"unrelated": [True]}),
            sibling_staging / "unrelated" / "part-00000.parquet",
        )
        sibling = external.commit(
            sibling_staging,
            artifact_name="unrelated",
            artifact_type="project.unrelated-sibling.v1",
        )
        outputs["unrelated_sibling"] = {
            "unrelated": sibling.artifact_ref.to_dict()
        }
    validity = _commit_validity(external)
    outputs["validity"] = {"validity": validity.artifact_ref.to_dict()}
    node_ids = tuple(outputs)
    dag = {
        "contract_version": "test",
        "nodes": [{"node_id": item} for item in node_ids],
        "edges": [{"from": "data_input", "to": node_id}],
    }
    event_chain_head = _write_succeeded_events(
        run_root,
        run_id=HASH_A,
        node_ids=node_ids,
    )
    record = {
        "contract_version": "research-runtime-operator-dag-run-v3",
        "status": "succeeded",
        "project_id": f"research_package_{project_id}",
        "run_id": HASH_A,
        "dag_id": typed_canonical_hash(dag),
        "dag": dag,
        "root_seed": 7,
        "fixed_clock": "2026-01-31T16:00:00+08:00",
        "mode": "deterministic_serial",
        "audit_environment": {"python": "test", "backend": "pyarrow"},
        "audit_manifest_digest": HASH_C,
        "reused_nodes": [],
        "outputs": outputs,
        "completion_metadata": {
            "artifact_hashes": {},
            "proof_hashes": {},
            "counts": {},
            "backend_id": None,
            "fidelity": None,
            "limitations": [],
            "contract_version": "research-runtime-completion-metadata-v1",
        },
        "event_chain_head": event_chain_head,
    }
    (run_root / "operator-dag-run.json").write_text(
        canonical_json(record), encoding="utf-8",
    )
    table_id = f"{project_id}_metrics"
    spec = ResultSpec.build((ResultTableSpec(
        table_id=table_id,
        role="primary",
        source_node_id=node_id,
        source_port="result",
        artifact_type=artifact_type,
        schema_id=schema_id,
        path_prefix="metrics",
    ),))
    proof = MetricReachabilityProof.build(
        definition,
        (node_id, "result", artifact_type),
        (table_id, schema_id, "metrics"),
    )
    bundle_path = _verifier_bundle(
        tmp_path,
        project_id=project_id,
        verifier_id=verifier_id,
        schema_id=schema_id,
        metric_definitions=(definition,),
    )
    verifier = ProjectVerifierBundleManifest.from_dict(
        json.loads((bundle_path / "manifest.json").read_text(encoding="utf-8"))
    )
    result_root = tmp_path / "results"
    _, directory, _ = ResultAssembler(result_root).finalize(
        run_root=run_root,
        project_id=f"research_package_{project_id}",
        package_hash=HASH_B,
        plan_hash=HASH_C,
        result_spec=spec,
        catalog_hashes={"prices": HASH_D},
        data_references={"prices": _data_reference()},
        metric_proofs=(proof,),
        implementation_manifest_hash=HASH_A,
        verification_policy_id=policy_id_for_claim("research_observation"),
        validity_producer_hash=VALIDITY_FACTS_PRODUCER_HASH,
        verifier_identity=verifier.identity(),
    )
    return verify_result(
        directory,
        result_store=result_root,
        verifier_bundle=bundle_path, project_verifier_process_slots=3 if os.name == "nt" else 2
    )


def _finalize(tmp_path: Path, *, phase_hook=None):
    run_root, result_root, object_root = _runtime_fixture(tmp_path)
    bundle, directory, reference = ResultAssembler(result_root).finalize(
        run_root=run_root,
        project_id="research_package_test",
        package_hash=HASH_B,
        plan_hash=HASH_C,
        result_spec=_spec(),
        catalog_hashes={"prices": HASH_D},
        data_references={"prices": _data_reference()},
        metric_proofs=(_proof(),),
        implementation_manifest_hash=HASH_A,
        verification_policy_id=policy_id_for_claim("research_observation"),
        validity_producer_hash=VALIDITY_FACTS_PRODUCER_HASH,
        phase_hook=phase_hook,
    )
    return run_root, result_root, object_root, bundle, directory, reference


def _verifier_bundle(tmp_path: Path, *, project_id: str, verifier_id: str,
                     schema_id: str, behavior: str = "pass",
                     metric_definitions: tuple[MetricDefinition, ...] = (),
                     raise_error: bool = False,
                     statistics_matrix_evidence: dict[str, object] | None = None,
                     ) -> Path:
    source = tmp_path / "verifier_source"
    source.mkdir(parents=True)
    output_version = (
        "project-verifier-output-v2"
        if statistics_matrix_evidence is not None
        else "project-verifier-output-v1"
    )
    matrix_evidence_line = (
        f"        'statistics_matrix_evidence': {statistics_matrix_evidence!r},\n"
        if statistics_matrix_evidence is not None
        else ""
    )
    outcome_body = (
        "    raise RuntimeError('project verifier failed')\n"
        if raise_error
        else (
            "    return {\n"
            f"        'contract_version': {output_version!r},\n"
            f"        'status': {behavior!r},\n"
            "        'result_id': context['result_id'],\n"
            f"        'findings': {['项目复核未通过'] if behavior == 'fail' else []!r},\n"
            "        'evidence_hashes': {},\n"
            + matrix_evidence_line
            +
            "    }\n"
        )
    )
    (source / "check.py").write_text(
        "import json\n"
        "from pathlib import Path\n\n"
        "def verify(context, input_root):\n"
        "    inputs = json.loads((input_root / 'manifest.json').read_text(encoding='utf-8'))\n"
        f"    assert [table['schema_id'] for table in inputs['tables']] == [{schema_id!r}]\n"
        "    assert not inputs['support_files']\n"
        "    assert len(list((input_root / 'tables').rglob('*.parquet'))) == 1\n"
        + outcome_body,
        encoding="utf-8",
    )
    return compile_project_verifier_bundle(
        source_root=source,
        output_root=tmp_path / "verifier_bundles",
        project_id=project_id,
        verifier_id=verifier_id,
        verifier_version="1.0.0",
        entry_module="check",
        entry_function="verify",
        authorized_schema_ids=(schema_id,),
        metric_definitions=metric_definitions,
        dependency_lock={},
    )


def _file_snapshot(root: Path) -> dict[str, bytes]:
    return {
        item.relative_to(root).as_posix(): item.read_bytes()
        for item in sorted(root.rglob("*"))
        if item.is_file()
    }


def _project_metric(
    metric_id: str, *, implementation_digest: str = HASH_D,
) -> MetricDefinition:
    return MetricDefinition.build(
        metric_id=metric_id,
        version="1.0.0",
        input_artifact_type="data.columnar-bundle.v1",
        result_schema_id="data.columnar-bundle.metrics.v1",
        output_schema={"value": "float64"},
        unit="decimal_return",
        frequency="bounded_sample",
        annualization_policy="none",
        risk_free_rate_policy="not_applicable",
        null_policy="forbid",
        direction="higher_is_better",
        implementation_ref=f"{metric_id}.value",
        implementation_digest=implementation_digest,
        measurement_semantics={
            "quantity": "project_fixture_metric",
            "numerator": "fixture_value",
            "denominator": "not_applicable",
            "observation_timing": "bounded_sample_close",
            "aggregation": "single_value",
        },
    )


def test_new_project_verifier_rejects_legacy_metric_definition(
    tmp_path: Path,
) -> None:
    payload = _project_metric("project.legacy_metric").payload()
    payload.pop("measurement_semantics")
    payload["contract_version"] = "research-metric-definition-v2"
    payload["definition_digest"] = typed_canonical_hash(payload)
    legacy = MetricDefinition.from_dict(payload)

    with pytest.raises(
        ExtensionError,
        match="新建项目 Verifier 必须使用当前 MetricDefinition 合同",
    ):
        _verifier_bundle(
            tmp_path,
            project_id="logical-project",
            verifier_id="legacy-check",
            schema_id="data.columnar-bundle.metrics.v1",
            metric_definitions=(legacy,),
        )


@pytest.mark.parametrize("behavior", ["pass", "fail"])
def test_project_verifier_result_closure_and_independent_outcome(
    tmp_path: Path, behavior: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PYTHONDONTWRITEBYTECODE", raising=False)
    bundle_path = _verifier_bundle(
        tmp_path, project_id="logical-project", verifier_id="alpha-check",
        schema_id="data.columnar-bundle.metrics.v1", behavior=behavior,
    )
    bundle_before = _file_snapshot(bundle_path)
    verifier_identity = ProjectVerifierBundleManifest.from_dict(
        json.loads((bundle_path / "manifest.json").read_text(encoding="utf-8"))
    ).identity()
    run_root, result_root, _ = _runtime_fixture(tmp_path)
    bundle, directory, _ = ResultAssembler(result_root).finalize(
        run_root=run_root,
        project_id="research_package_test",
        package_hash=HASH_B,
        plan_hash=HASH_C,
        result_spec=_spec(),
        catalog_hashes={"prices": HASH_D},
        data_references={"prices": _data_reference()},
        metric_proofs=(_proof(),),
        implementation_manifest_hash=HASH_A,
        verification_policy_id=policy_id_for_claim("research_observation"),
        validity_producer_hash=VALIDITY_FACTS_PRODUCER_HASH,
        verifier_identity=verifier_identity,
    )
    with pytest.raises(EvidenceContractError, match="必须显式提供 bundle"):
        verify_result(directory, result_store=result_root, project_verifier_process_slots=3 if os.name == "nt" else 2)
    output = tmp_path / "verification.json"
    context = verify_result(
        directory, result_store=result_root,
        verifier_bundle=bundle_path, output=output, project_verifier_process_slots=3 if os.name == "nt" else 2
    )
    assert context.verification.project_verifier_identity == verifier_identity
    assert context.verification.status == behavior
    assert load_verified_result_context(output, result_store=result_root).verification == context.verification
    verify_project_verifier_bundle(bundle_path)
    second = verify_result(
        directory,
        result_store=result_root,
        verifier_bundle=bundle_path,
        output=tmp_path / "verification-second.json", project_verifier_process_slots=3 if os.name == "nt" else 2
    )
    assert second.verification.status == behavior
    assert _file_snapshot(bundle_path) == bundle_before
    assert not tuple(bundle_path.rglob("__pycache__"))
    assert not tuple(bundle_path.rglob("*.pyc"))


def test_project_verifier_v2_matrix_evidence_enters_statistics_gate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PYTHONDONTWRITEBYTECODE", raising=False)
    bundle_path = _verifier_bundle(
        tmp_path,
        project_id="logical-project",
        verifier_id="matrix-check",
        schema_id="data.columnar-bundle.metrics.v1",
        statistics_matrix_evidence={
            "contract_version": "research-statistics-matrix-evidence-v1",
            "matrix_rows": 4,
            "matrix_effective_rows": 4,
            "matrix_columns": 3,
            "matrix_rank": 3,
        },
    )
    verifier_identity = verify_project_verifier_bundle(bundle_path).identity()
    run_root, result_root, _ = _runtime_fixture(tmp_path)
    _, directory, _ = ResultAssembler(result_root).finalize(
        run_root=run_root,
        project_id="research_package_test",
        package_hash=HASH_B,
        plan_hash=HASH_C,
        result_spec=_spec(),
        catalog_hashes={"prices": HASH_D},
        data_references={"prices": _data_reference()},
        metric_proofs=(_proof(),),
        implementation_manifest_hash=HASH_A,
        verification_policy_id=policy_id_for_claim("research_observation"),
        validity_producer_hash=VALIDITY_FACTS_PRODUCER_HASH,
        verifier_identity=verifier_identity,
    )

    context = verify_result(
        directory,
        result_store=result_root,
        verifier_bundle=bundle_path, project_verifier_process_slots=3 if os.name == "nt" else 2
    )

    assert context.verification.status == "pass"
    assert context.verification.project_verifier_identity == verifier_identity


def test_new_project_result_embeds_plan_verifier_and_verifies_without_external_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PYTHONDONTWRITEBYTECODE", raising=False)
    bundle_path = _verifier_bundle(
        tmp_path,
        project_id="logical-project",
        verifier_id="embedded-check",
        schema_id="data.columnar-bundle.metrics.v1",
    )
    manifest = verify_project_verifier_bundle(bundle_path)
    run_root, result_root, _ = _runtime_fixture(tmp_path)
    bundle, directory, _ = ResultAssembler(result_root).finalize(
        run_root=run_root,
        project_id="research_package_test",
        package_hash=HASH_B,
        plan_hash=HASH_C,
        result_spec=_spec(),
        catalog_hashes={"prices": HASH_D},
        data_references={"prices": _data_reference()},
        metric_proofs=(_proof(),),
        implementation_manifest_hash=HASH_A,
        verification_policy_id=policy_id_for_claim("research_observation"),
        validity_producer_hash=VALIDITY_FACTS_PRODUCER_HASH,
        verifier_identity=manifest.identity(),
        verifier_bundle_source=bundle_path,
    )

    embedded = directory / "verifiers" / manifest.bundle_hash
    assert bundle.verification.verifier_bundle_path == (
        f"verifiers/{manifest.bundle_hash}"
    )
    assert verify_project_verifier_bundle(embedded).identity() == manifest.identity()
    context = verify_result(directory, result_store=result_root, project_verifier_process_slots=3 if os.name == "nt" else 2)
    assert context.verification.status == "pass"
    assert not tuple(embedded.rglob("__pycache__"))
    assert not tuple(embedded.rglob("*.pyc"))

    (embedded / "sources" / "check.py").write_text(
        "def verify(context, input_root):\n    return {}\n",
        encoding="utf-8",
    )
    with pytest.raises(ResultContractError, match="内嵌 Verifier bundle 无法复验"):
        verify_result(directory, result_store=result_root, project_verifier_process_slots=3 if os.name == "nt" else 2)


def test_project_verifier_execution_failure_does_not_mutate_bundle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PYTHONDONTWRITEBYTECODE", raising=False)
    bundle_path = _verifier_bundle(
        tmp_path,
        project_id="logical-project",
        verifier_id="failing-check",
        schema_id="data.columnar-bundle.metrics.v1",
        raise_error=True,
    )
    manifest = verify_project_verifier_bundle(bundle_path)
    bundle_before = _file_snapshot(bundle_path)
    run_root, result_root, _ = _runtime_fixture(tmp_path)
    _, directory, _ = ResultAssembler(result_root).finalize(
        run_root=run_root,
        project_id="research_package_test",
        package_hash=HASH_B,
        plan_hash=HASH_C,
        result_spec=_spec(),
        catalog_hashes={"prices": HASH_D},
        data_references={"prices": _data_reference()},
        metric_proofs=(_proof(),),
        implementation_manifest_hash=HASH_A,
        verification_policy_id=policy_id_for_claim("research_observation"),
        validity_producer_hash=VALIDITY_FACTS_PRODUCER_HASH,
        verifier_identity=manifest.identity(),
    )

    with pytest.raises(ResultContractError, match="project_verifier_worker_failed"):
        verify_result(
            directory,
            result_store=result_root,
            verifier_bundle=bundle_path, project_verifier_process_slots=3 if os.name == "nt" else 2
        )

    verify_project_verifier_bundle(bundle_path)
    assert _file_snapshot(bundle_path) == bundle_before
    assert not tuple(bundle_path.rglob("__pycache__"))
    assert not tuple(bundle_path.rglob("*.pyc"))


def test_project_verifier_rejects_wrong_bundle_and_missing_schema(tmp_path: Path) -> None:
    first_metric = _project_metric("project.alpha_score")
    second_metric = _project_metric("project.beta_ratio")
    bundle_path = _verifier_bundle(
        tmp_path / "first", project_id="project-a", verifier_id="alpha-check",
        schema_id="data.columnar-bundle.metrics.v1", metric_definitions=(first_metric,),
    )
    other_path = _verifier_bundle(
        tmp_path / "second", project_id="project-b", verifier_id="beta-check",
        schema_id="data.columnar-bundle.metrics.v1", metric_definitions=(second_metric,),
    )
    (tmp_path / "result").mkdir()
    run_root, result_root, _ = _runtime_fixture(tmp_path / "result")
    identity = json.loads((bundle_path / "manifest.json").read_text(encoding="utf-8"))
    from research_pipeline.extensions import verify_project_verifier_bundle
    manifest = verify_project_verifier_bundle(bundle_path)
    _, directory, _ = ResultAssembler(result_root).finalize(
        run_root=run_root, project_id="research_package_test",
        package_hash=HASH_B, plan_hash=HASH_C, result_spec=_spec(),
        catalog_hashes={"prices": HASH_D}, data_references={"prices": _data_reference()},
        metric_proofs=(_proof(),), implementation_manifest_hash=HASH_A,
        verification_policy_id=policy_id_for_claim("research_observation"),
        validity_producer_hash=VALIDITY_FACTS_PRODUCER_HASH,
        verifier_identity=manifest.identity(),
    )
    with pytest.raises(ResultContractError, match="冻结身份不一致"):
        verify_result(directory, result_store=result_root, verifier_bundle=other_path, project_verifier_process_slots=3 if os.name == "nt" else 2)
    malformed = dict(identity)
    malformed["source_files"] = ["invalid"]
    with pytest.raises(ExtensionError, match="source_files 条目"):
        ProjectVerifierBundleManifest.from_dict(malformed)


def test_project_verifier_bundle_is_frozen_into_plan_closure(tmp_path: Path) -> None:
    bundle_path = _verifier_bundle(
        tmp_path,
        project_id="logical-project",
        verifier_id="closure-check",
        schema_id="data.columnar-bundle.metrics.v1",
    )
    from research_pipeline.extensions import admit_project_verifier_bundle

    verifier = admit_project_verifier_bundle(bundle_path)
    target = tmp_path / "plan"
    target.mkdir()
    plan = SimpleNamespace(verifier_identity=verifier.identity)

    _copy_verifier_bundle_closure(
        target=target,
        plan=plan,
        verifier=verifier,
    )
    _verify_plan_verifier_bundle(
        root=target,
        verifier_admission=verifier.identity,
    )
    assert sorted((target / "verifiers").iterdir()) == [
        target / "verifiers" / verifier.manifest.bundle_hash
    ]


def test_project_metric_is_result_scoped_and_does_not_enter_public_registry(
    tmp_path: Path,
) -> None:
    definition = _project_metric("project.alpha_score")
    bundle_path = _verifier_bundle(
        tmp_path, project_id="project-a", verifier_id="alpha-check",
        schema_id=definition.result_schema_id, metric_definitions=(definition,),
    )
    manifest = ProjectVerifierBundleManifest.from_dict(
        json.loads((bundle_path / "manifest.json").read_text(encoding="utf-8"))
    )
    run_root, result_root, _ = _runtime_fixture(
        tmp_path, metric_ref=definition.metric_ref,
    )
    _, directory, _ = ResultAssembler(result_root).finalize(
        run_root=run_root, project_id="research_package_test",
        package_hash=HASH_B, plan_hash=HASH_C, result_spec=_spec(),
        catalog_hashes={"prices": HASH_D}, data_references={"prices": _data_reference()},
        metric_proofs=(_proof(definition),), implementation_manifest_hash=HASH_A,
        verification_policy_id=policy_id_for_claim("research_observation"),
        validity_producer_hash=VALIDITY_FACTS_PRODUCER_HASH,
        verifier_identity=manifest.identity(),
    )
    context = verify_result(
        directory, result_store=result_root, verifier_bundle=bundle_path, project_verifier_process_slots=3 if os.name == "nt" else 2
    )
    assert [item.metric_ref for item in context.metrics] == [definition.metric_ref]
    assert compare_verification_results(context, context).comparable is True
    from research_pipeline.platform.metric_contracts import (
        UnknownMetricError,
        build_mainline_metric_registry,
    )
    with pytest.raises(UnknownMetricError):
        build_mainline_metric_registry().require(definition.metric_ref)


def test_project_metric_definition_drift_makes_verified_results_incomparable(
    tmp_path: Path,
) -> None:
    contexts = []
    for project_id, implementation_digest in (
        ("alpha-project", HASH_C),
        ("beta-project", HASH_D),
    ):
        project_root = tmp_path / project_id
        definition = _project_metric(
            "project.common_score",
            implementation_digest=implementation_digest,
        )
        bundle_path = _verifier_bundle(
            project_root,
            project_id=project_id,
            verifier_id=f"{project_id}-check",
            schema_id=definition.result_schema_id,
            metric_definitions=(definition,),
        )
        manifest = ProjectVerifierBundleManifest.from_dict(
            json.loads((bundle_path / "manifest.json").read_text(encoding="utf-8"))
        )
        run_root, result_root, _ = _runtime_fixture(
            project_root,
            metric_ref=definition.metric_ref,
        )
        _, directory, _ = ResultAssembler(result_root).finalize(
            run_root=run_root,
            project_id="research_package_test",
            package_hash=HASH_B,
            plan_hash=HASH_C,
            result_spec=_spec(),
            catalog_hashes={"prices": HASH_D},
            data_references={"prices": _data_reference()},
            metric_proofs=(_proof(definition),),
            implementation_manifest_hash=HASH_A,
            verification_policy_id=policy_id_for_claim("research_observation"),
            validity_producer_hash=VALIDITY_FACTS_PRODUCER_HASH,
            verifier_identity=manifest.identity(),
        )
        contexts.append(verify_result(
            directory,
            result_store=result_root,
            verifier_bundle=bundle_path, project_verifier_process_slots=3 if os.name == "nt" else 2
        ))

    comparison = compare_verification_results(*contexts)
    assert comparison.comparable is False
    assert comparison.reason_codes == ("comparison.metric_definition_mismatch",)
    assert comparison.metric_comparisons == ()


def test_two_heterogeneous_project_closures_share_only_public_abi(
    tmp_path: Path,
) -> None:
    alpha_definition = MetricDefinition.build(
        metric_id="project.alpha_score",
        version="1.0.0",
        input_artifact_type="project.alpha-artifact.v1",
        result_schema_id="project.alpha.metrics.v1",
        output_schema={"value": "float64", "alpha_group": "string"},
        unit="score",
        frequency="event_window",
        annualization_policy="none",
        risk_free_rate_policy="not_applicable",
        null_policy="forbid",
        direction="higher_is_better",
        implementation_ref="alpha.score",
        implementation_digest=HASH_C,
        measurement_semantics={
            "quantity": "alpha_score",
            "numerator": "alpha_signal",
            "denominator": "not_applicable",
            "observation_timing": "event_window_close",
            "aggregation": "single_value",
        },
    )
    beta_definition = MetricDefinition.build(
        metric_id="project.beta_ratio",
        version="1.0.0",
        input_artifact_type="project.beta-panel.v1",
        result_schema_id="project.beta.metrics.v1",
        output_schema={"value": "float64", "beta_window": "int64"},
        unit="ratio",
        frequency="monthly_panel",
        annualization_policy="none",
        risk_free_rate_policy="not_applicable",
        null_policy="forbid",
        direction="lower_is_better",
        implementation_ref="beta.ratio",
        implementation_digest=HASH_D,
        measurement_semantics={
            "quantity": "beta_ratio",
            "numerator": "beta_events",
            "denominator": "beta_windows",
            "observation_timing": "monthly_panel_close",
            "aggregation": "ratio",
        },
    )
    alpha = _heterogeneous_project_context(
        tmp_path / "alpha",
        project_id="alpha-project",
        verifier_id="alpha-check",
        node_id="alpha_node",
        artifact_type=alpha_definition.input_artifact_type,
        schema_id=alpha_definition.result_schema_id,
        definition=alpha_definition,
        extra_column=("alpha_group", pa.array(["A"])),
        include_sibling=False,
    )
    beta = _heterogeneous_project_context(
        tmp_path / "beta",
        project_id="beta-project",
        verifier_id="beta-check",
        node_id="beta_panel_node",
        artifact_type=beta_definition.input_artifact_type,
        schema_id=beta_definition.result_schema_id,
        definition=beta_definition,
        extra_column=("beta_window", pa.array([20], type=pa.int64())),
        include_sibling=True,
    )

    assert alpha.verification.status == beta.verification.status == "pass"
    assert alpha.metrics[0].metric_ref == alpha_definition.metric_ref
    assert beta.metrics[0].metric_ref == beta_definition.metric_ref
    assert alpha.snapshot.bundle.tables[0].artifact_type != (
        beta.snapshot.bundle.tables[0].artifact_type
    )
    assert set(alpha.snapshot.read_table(alpha_definition.result_schema_id).schema.names) != set(
        beta.snapshot.read_table(beta_definition.result_schema_id).schema.names
    )
    assert set(alpha.verification.project_verifier_identity) == set(
        beta.verification.project_verifier_identity
    )
    assert alpha.verification.project_verifier_identity["verifier_id"] != (
        beta.verification.project_verifier_identity["verifier_id"]
    )
    assert len(alpha.snapshot.bundle.verification.run.node_statuses) == 3
    assert len(beta.snapshot.bundle.verification.run.node_statuses) == 4


def test_result_bundle_finalize_is_unique_immutable_and_idempotent(tmp_path: Path) -> None:
    run_root, result_root, _, bundle, directory, reference = _finalize(tmp_path)
    assert directory == result_root / bundle.project_id / bundle.run_id / bundle.result_id
    assert set(path.name for path in directory.iterdir()) == {
        "result.json", "COMMITTED", "tables", "support",
    }
    assert len(bundle.support_files) == 1
    assert bundle.support_files[0].artifact_type == "research.validity-facts.v1"
    assert json.loads((run_root / "result-ref.json").read_text(encoding="utf-8")) == reference.to_dict()
    loaded = ResultStore(result_root, create=False).verify(directory)
    assert loaded == bundle
    again = ResultAssembler(result_root).finalize(
        run_root=run_root,
        project_id=bundle.project_id,
        package_hash=bundle.package_hash,
        plan_hash=bundle.plan_hash,
        result_spec=bundle.result_spec,
        catalog_hashes=bundle.catalog_hashes,
        data_references={"prices": _data_reference()},
        metric_proofs=bundle.metric_proofs,
        implementation_manifest_hash=bundle.implementation_manifest_hash,
        verification_policy_id=bundle.verification.policy_id,
        validity_producer_hash=bundle.verification.validity_producer_hash,
    )
    assert again[0] == bundle
    assert len(tuple((result_root / bundle.project_id / bundle.run_id).iterdir())) == 1


def test_result_identity_is_stable_across_equivalent_event_chains(
    tmp_path: Path,
) -> None:
    first_run, first_result_root, _ = _runtime_fixture(tmp_path / "first")
    second_run, second_result_root, _ = _runtime_fixture(tmp_path / "second")
    first_record = json.loads(
        (first_run / "operator-dag-run.json").read_text(encoding="utf-8")
    )
    second_record = json.loads(
        (second_run / "operator-dag-run.json").read_text(encoding="utf-8")
    )
    assert first_record["event_chain_head"] != second_record["event_chain_head"]
    assert first_record["outputs"] == second_record["outputs"]
    assert operator_dag_runtime_hash(first_record) == operator_dag_runtime_hash(
        second_record
    )

    finalize_kwargs = {
        "project_id": "research_package_test",
        "package_hash": HASH_B,
        "plan_hash": HASH_C,
        "result_spec": _spec(),
        "catalog_hashes": {"prices": HASH_D},
        "data_references": {"prices": _data_reference()},
        "metric_proofs": (_proof(),),
        "implementation_manifest_hash": HASH_A,
        "verification_policy_id": policy_id_for_claim("research_observation"),
        "validity_producer_hash": VALIDITY_FACTS_PRODUCER_HASH,
    }
    first_bundle, first_directory, _ = ResultAssembler(first_result_root).finalize(
        run_root=first_run,
        **finalize_kwargs,
    )
    second_bundle, second_directory, _ = ResultAssembler(second_result_root).finalize(
        run_root=second_run,
        **finalize_kwargs,
    )

    assert first_bundle == second_bundle
    assert first_bundle.result_id == second_bundle.result_id
    assert (first_directory / "result.json").read_bytes() == (
        second_directory / "result.json"
    ).read_bytes()


def test_runtime_hash_changes_when_formal_outputs_change(tmp_path: Path) -> None:
    run_root, _, _ = _runtime_fixture(tmp_path)
    record = json.loads(
        (run_root / "operator-dag-run.json").read_text(encoding="utf-8")
    )
    changed = json.loads(canonical_json(record))
    changed["outputs"]["validity"]["validity"]["content_hash"] = "f" * 64

    assert operator_dag_runtime_hash(record) != operator_dag_runtime_hash(changed)


def test_result_finalize_still_rejects_runtime_event_chain_drift(
    tmp_path: Path,
) -> None:
    run_root, result_root, _ = _runtime_fixture(tmp_path)
    record_path = run_root / "operator-dag-run.json"
    record = json.loads(record_path.read_text(encoding="utf-8"))
    record["event_chain_head"] = "f" * 64
    record_path.write_text(canonical_json(record), encoding="utf-8")

    with pytest.raises(ResultContractError, match="无法形成终态运行摘要"):
        ResultAssembler(result_root).finalize(
            run_root=run_root,
            project_id="research_package_test",
            package_hash=HASH_B,
            plan_hash=HASH_C,
            result_spec=_spec(),
            catalog_hashes={"prices": HASH_D},
            data_references={"prices": _data_reference()},
            metric_proofs=(_proof(),),
            implementation_manifest_hash=HASH_A,
            verification_policy_id=policy_id_for_claim("research_observation"),
            validity_producer_hash=VALIDITY_FACTS_PRODUCER_HASH,
        )


def test_result_assembler_rejects_formal_causal_output_without_parquet(
    tmp_path: Path,
) -> None:
    run_root, result_root, _ = _runtime_fixture(tmp_path)
    external = ExternalArtifactStore(run_root / "external-artifacts")
    staging = external.prepare()
    (staging / "features").mkdir()
    (staging / "features" / "metadata.json").write_text(
        "{}",
        encoding="utf-8",
    )
    feature = external.commit(
        staging,
        artifact_name="features",
        artifact_type="research.feature-set.v1",
    )
    record_path = run_root / "operator-dag-run.json"
    record = json.loads(record_path.read_text(encoding="utf-8"))
    record["outputs"]["feature"] = {
        "features": feature.artifact_ref.to_dict(),
    }
    record_path.write_text(canonical_json(record), encoding="utf-8")
    spec = ResultSpec.build(
        (
            *_spec().tables,
            ResultTableSpec(
                table_id="causal-time.feature.features",
                role="diagnostic",
                source_node_id="feature",
                source_port="features",
                artifact_type="research.feature-set.v1",
                schema_id="research.causal-time.feature.features.v2",
                path_prefix="features",
            ),
        )
    )

    with pytest.raises(ResultContractError, match="没有选中正式 Parquet 表"):
        ResultAssembler(result_root).finalize(
            run_root=run_root,
            project_id="research_package_test",
            package_hash=HASH_B,
            plan_hash=HASH_C,
            result_spec=spec,
            catalog_hashes={"prices": HASH_D},
            data_references={"prices": _data_reference()},
            metric_proofs=(_proof(),),
            implementation_manifest_hash=HASH_A,
            verification_policy_id=policy_id_for_claim("research_observation"),
            validity_producer_hash=VALIDITY_FACTS_PRODUCER_HASH,
        )


@pytest.mark.parametrize("attack", ["request_id", "claim_ceiling"])
def test_result_assembler_rejects_runtime_claim_lineage_drift(
    tmp_path: Path,
    attack: str,
) -> None:
    facts = default_passing_validity_facts()
    data_pit = facts["data_pit"]
    assert isinstance(data_pit, dict)
    if attack == "request_id":
        data_pit["consumed_request_ids"] = ["other_prices"]
        data_pit["input_claim_ceilings"] = {
            "other_prices": "tradable_simulation",
        }
    else:
        data_pit["input_claim_ceilings"] = {
            "prices": "research_observation",
        }
        data_pit["effective_claim_ceiling"] = "research_observation"
    run_root, result_root, _ = _runtime_fixture(
        tmp_path,
        validity_payload=facts,
    )

    with pytest.raises(ResultContractError, match="claim lineage"):
        ResultAssembler(result_root).finalize(
            run_root=run_root,
            project_id="research_package_test",
            package_hash=HASH_B,
            plan_hash=HASH_C,
            result_spec=_spec(),
            catalog_hashes={"prices": HASH_D},
            data_references={"prices": _data_reference()},
            metric_proofs=(_proof(),),
            implementation_manifest_hash=HASH_A,
            verification_policy_id=policy_id_for_claim("research_observation"),
            validity_producer_hash=VALIDITY_FACTS_PRODUCER_HASH,
            formal_input_request_ids=("prices",),
            input_claim_ceilings={"prices": "tradable_simulation"},
        )


def test_verifier_rejects_claim_lineage_synchronized_only_in_validity(
    tmp_path: Path,
) -> None:
    _, _, _, bundle, _, _ = _finalize(tmp_path)
    facts = default_passing_validity_facts()
    data_pit = facts["data_pit"]
    assert isinstance(data_pit, dict)
    data_pit["input_claim_ceilings"] = {
        "prices": "research_observation",
    }
    data_pit["effective_claim_ceiling"] = "research_observation"

    with pytest.raises(EvidenceContractError, match="claim lineage"):
        _verify_input_claim_lineage(bundle, facts)


def test_result_assembler_requires_daily_etf_financial_context() -> None:
    control_paths = (
        "simulation/result-contract/manifest.json",
        "simulation/result-contract/COMMITTED",
        "simulation/tca/manifest.json",
        "simulation/tca/COMMITTED",
        "simulation/tca/oracle-input.json",
    )
    commit = SimpleNamespace(
        semantic_hash=HASH_A,
        files={path: HASH_B for path in control_paths},
    )
    validity = ResultSupportFile(
        HASH_C,
        "research.validity-facts.v1",
        "result.json",
        f"support/{HASH_C}/result.json",
        HASH_D,
    )
    result_spec = ResultSpec.build(tuple(
        ResultTableSpec(
            f"canonical_{name}",
            "primary" if name == "orders" else "diagnostic",
            "simulation",
            "simulation",
            "research.daily-simulation.v1",
            schema_id,
            f"simulation/result-contract/{name}",
        )
        for name, schema_id in CANONICAL_SIMULATION_SCHEMA_IDS.items()
    ) + tuple(
        ResultTableSpec(
            f"tca_{name}",
            "diagnostic",
            "simulation",
            "simulation",
            "research.daily-simulation.v1",
            schema_id,
            f"simulation/tca/{name}",
        )
        for name, schema_id in BAR_TCA_SCHEMA_IDS.items()
    ))

    class ManifestReader:
        @staticmethod
        def read_bytes(_commit, relative_path: str) -> bytes:
            assert relative_path == "simulation/result-contract/manifest.json"
            return canonical_json({
                "semantics": {"asset_class": "cn_etf", "frequency": "daily"}
            }).encode("utf-8")

    with pytest.raises(ResultContractError, match="ETF 日频 Result 缺少"):
        ResultAssembler._support_files(
            result_spec,
            {HASH_A: commit},
            external=ManifestReader(),
            validity_support=validity,
        )

    commit.files["simulation/daily-context.json"] = HASH_D
    selected = ResultAssembler._support_files(
        result_spec,
        {HASH_A: commit},
        external=ManifestReader(),
        validity_support=validity,
    )
    assert "simulation/daily-context.json" in {
        item.source_path for item in selected
    }


def test_result_bundle_reports_atomic_publish_before_reference_write(
    tmp_path: Path,
) -> None:
    run_root, result_root, _ = _runtime_fixture(tmp_path)
    published = []
    assembler = ResultAssembler(result_root)
    kwargs = {
        "run_root": run_root,
        "project_id": "research_package_test",
        "package_hash": HASH_B,
        "plan_hash": HASH_C,
        "result_spec": _spec(),
        "catalog_hashes": {"prices": HASH_D},
        "data_references": {"prices": _data_reference()},
        "metric_proofs": (_proof(),),
        "implementation_manifest_hash": HASH_A,
        "verification_policy_id": policy_id_for_claim("research_observation"),
        "validity_producer_hash": VALIDITY_FACTS_PRODUCER_HASH,
        "published_hook": lambda bundle, directory: published.append(
            (bundle.result_id, directory)
        ),
    }

    bundle, directory, _ = assembler.finalize(**kwargs)
    assert published == [(bundle.result_id, directory)]

    published.clear()
    same_bundle, same_directory, _ = assembler.finalize(**kwargs)
    assert same_bundle == bundle
    assert same_directory == directory
    assert published == [(bundle.result_id, directory)]


def test_result_bundle_rejects_validity_gates_summary(tmp_path: Path) -> None:
    run_root, result_root, _ = _runtime_fixture(
        tmp_path,
        validity_artifact_type="research.validity-gates.v1",
    )

    with pytest.raises(ResultContractError, match="唯一 validity facts"):
        ResultAssembler(result_root).finalize(
            run_root=run_root,
            project_id="research_package_test",
            package_hash=HASH_B,
            plan_hash=HASH_C,
            result_spec=_spec(),
            catalog_hashes={"prices": HASH_D},
            data_references={"prices": _data_reference()},
            metric_proofs=(_proof(),),
            implementation_manifest_hash=HASH_A,
            verification_policy_id=policy_id_for_claim("research_observation"),
            validity_producer_hash=VALIDITY_FACTS_PRODUCER_HASH,
        )


def test_result_bundle_rejects_relabelled_validity_gates_summary(
    tmp_path: Path,
) -> None:
    run_root, result_root, _ = _runtime_fixture(tmp_path)
    record_path = run_root / "operator-dag-run.json"
    record = json.loads(record_path.read_text(encoding="utf-8"))
    external = ExternalArtifactStore(run_root / "external-artifacts")
    relabelled = _commit_validity(
        external,
        payload={
            "contract_version": "research-validity-gates-v1",
            "gate_ids": ["data.pit"],
            "issues": {"data.pit": []},
            "status": "pass",
        },
    )
    record["outputs"]["validity"]["validity"] = (
        relabelled.artifact_ref.to_dict()
    )
    record_path.write_text(canonical_json(record), encoding="utf-8")

    with pytest.raises(ResultContractError, match="不能封存 gates 摘要"):
        ResultAssembler(result_root).finalize(
            run_root=run_root,
            project_id="research_package_test",
            package_hash=HASH_B,
            plan_hash=HASH_C,
            result_spec=_spec(),
            catalog_hashes={"prices": HASH_D},
            data_references={"prices": _data_reference()},
            metric_proofs=(_proof(),),
            implementation_manifest_hash=HASH_A,
            verification_policy_id=policy_id_for_claim("research_observation"),
            validity_producer_hash=VALIDITY_FACTS_PRODUCER_HASH,
        )


@pytest.mark.parametrize("attack", ["result", "extra", "table"])
def test_result_bundle_tamper_fails_closed(tmp_path: Path, attack: str) -> None:
    run_root, result_root, object_root, bundle, directory, _ = _finalize(tmp_path)
    if attack == "result":
        payload = json.loads((directory / "result.json").read_text(encoding="utf-8"))
        payload["status"] = "verified"
        (directory / "result.json").write_text(canonical_json(payload), encoding="utf-8")
    elif attack == "extra":
        (directory / "extra.json").write_text("{}", encoding="utf-8")
    else:
        table_path = directory / next(iter(bundle.tables[0].files))
        with table_path.open("ab") as handle:
            handle.write(b"tamper")
    with pytest.raises(Exception):
        ResultStore(result_root, create=False).verify(directory)
    assert bundle.result_id


def test_result_bundle_accepts_hardlink_alias_and_still_rejects_tamper(tmp_path: Path) -> None:
    run_root, result_root, _, bundle, directory, _ = _finalize(tmp_path)
    alias = tmp_path / "result-alias.json"
    try:
        os.link(directory / "result.json", alias)
    except OSError:
        pytest.skip("当前文件系统不支持 hardlink 测试")
    assert ResultStore(result_root, create=False).verify(directory) == bundle
    alias.write_text("{}", encoding="utf-8")
    with pytest.raises(ResultContractError):
        ResultStore(result_root, create=False).verify(directory)


def test_result_bundle_rejects_symlinked_control_file(tmp_path: Path) -> None:
    run_root, result_root, _, _, directory, _ = _finalize(tmp_path)
    manifest = directory / "result.json"
    outside = tmp_path / "outside-result.json"
    outside.write_bytes(manifest.read_bytes())
    manifest.unlink()
    try:
        manifest.symlink_to(outside)
    except OSError as exc:
        pytest.skip(f"当前平台不允许创建文件 symlink: {type(exc).__name__}")
    with pytest.raises(ResultContractError, match="manifest 无法读取"):
        ResultStore(result_root, create=False).verify(directory)


def test_result_bundle_rejects_junction_or_directory_symlink(tmp_path: Path) -> None:
    run_root, result_root, _, _, directory, _ = _finalize(tmp_path)
    outside = tmp_path / "outside-result"
    directory.rename(outside)
    _create_directory_link(directory, outside)
    try:
        with pytest.raises(ResultContractError, match="路径安全验证失败"):
            ResultStore(result_root, create=False).verify(directory)
    finally:
        _remove_directory_link(directory)


def test_result_store_accepts_explicit_junction_root(tmp_path: Path) -> None:
    _, result_root, _, bundle, directory, _ = _finalize(tmp_path)
    linked_root = tmp_path / "result-store-link"
    _create_directory_link(linked_root, result_root)
    try:
        store = ResultStore(linked_root, create=False)
        assert store.verify(linked_root / directory.relative_to(result_root)) == bundle
    finally:
        _remove_directory_link(linked_root)


def test_result_bundle_prepared_failure_never_publishes(tmp_path: Path) -> None:
    run_root, result_root, _object_root = _runtime_fixture(tmp_path)

    def fail_after_prepare(phase: str) -> None:
        if phase == "prepared":
            raise RuntimeError("故障注入：staging 已完整准备")

    with pytest.raises(RuntimeError, match="故障注入"):
        ResultAssembler(result_root).finalize(
            run_root=run_root,
            project_id="research_package_test",
            package_hash=HASH_B,
            plan_hash=HASH_C,
            result_spec=_spec(),
            catalog_hashes={"prices": HASH_D},
            data_references={"prices": _data_reference()},
            metric_proofs=(_proof(),),
            implementation_manifest_hash=HASH_A,
            verification_policy_id=policy_id_for_claim("research_observation"),
            validity_producer_hash=VALIDITY_FACTS_PRODUCER_HASH,
            phase_hook=fail_after_prepare,
        )
    namespace = result_root / "research_package_test" / HASH_A
    assert not namespace.exists() or not tuple(namespace.iterdir())


def test_result_store_ignores_run_root_changes_after_finalize(tmp_path: Path) -> None:
    run_root, result_root, object_root, bundle, _, _ = _finalize(tmp_path)
    pq.write_table(
        pa.table({"metric": ["alpha"], "value": [999.0]}),
        object_root / "statistics" / "metrics.parquet",
    )
    (object_root / "result.json").write_text('{"tampered":true}', encoding="utf-8")
    store = ResultStore(result_root, create=False)
    table = store.read_table_by_schema_id(
        bundle, schema_id="data.columnar-bundle.metrics.v1"
    )
    assert table["value"].to_pylist() == [1.25]


def test_result_snapshot_reads_partition_schema_with_arrow_metadata(
    tmp_path: Path,
) -> None:
    schema_id = "research.metadata.fixture.v1"
    schema = pa.schema(
        [
            pa.field(
                "value",
                pa.int64(),
                metadata={b"unit": b"count"},
            )
        ],
        metadata={b"contract": b"fixture-v1"},
    )
    paths = ("part-00000.parquet", "part-00001.parquet")
    for index, relative_path in enumerate(paths):
        pq.write_table(
            pa.Table.from_arrays([pa.array([index])], schema=schema),
            tmp_path / relative_path,
        )
    manifest = SimpleNamespace(
        schema_id=schema_id,
        files={relative_path: HASH_A for relative_path in paths},
        row_counts={relative_path: 1 for relative_path in paths},
    )
    snapshot = ResultSnapshot(
        SimpleNamespace(tables=(manifest,)),
        tmp_path,
        {},
        {},
    )

    assert snapshot.table_schema(schema_id) == schema


def test_result_snapshot_counts_nested_projection_from_parquet_footer(
    tmp_path: Path,
) -> None:
    schema_id = "research.nested.fixture.v1"
    relative_path = "nested.parquet"
    table = pa.table({
        "nested": pa.array(
            [{"text": "alpha", "count": 1}, {"text": "beta", "count": 2}],
            type=pa.struct((
                pa.field("text", pa.string()),
                pa.field("count", pa.int64()),
            )),
        ),
        "other": pa.array([100, 200], type=pa.int64()),
    })
    pq.write_table(table, tmp_path / relative_path)
    manifest = SimpleNamespace(
        schema_id=schema_id,
        files={relative_path: HASH_A},
        row_counts={relative_path: table.num_rows},
    )
    snapshot = ResultSnapshot(
        SimpleNamespace(tables=(manifest,)),
        tmp_path,
        {},
        {},
    )
    metadata = pq.ParquetFile(tmp_path / relative_path).metadata
    expected_nested = sum(
        int(metadata.row_group(row_group).column(column).total_uncompressed_size)
        for row_group in range(metadata.num_row_groups)
        for column in range(metadata.row_group(row_group).num_columns)
        if metadata.row_group(row_group)
        .column(column)
        .path_in_schema.split(".", 1)[0]
        == "nested"
    )

    assert snapshot.table_uncompressed_bytes(
        schema_id,
        columns=("nested",),
    ) == expected_nested
    assert 0 < expected_nested < snapshot.table_uncompressed_bytes(schema_id)


def test_result_verify_hashes_in_chunks_and_uses_parquet_footer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _run_root, result_root, _, bundle, directory, _ = _finalize(tmp_path)
    table_path = (directory / next(iter(bundle.tables[0].files))).resolve()
    original_open = Path.open
    read_sizes: list[int] = []

    class TrackingFile:
        def __init__(self, handle) -> None:
            self._handle = handle

        def __enter__(self):
            self._handle.__enter__()
            return self

        def __exit__(self, *args):
            return self._handle.__exit__(*args)

        def read(self, size: int = -1):
            read_sizes.append(size)
            return self._handle.read(size)

        def __getattr__(self, name: str):
            return getattr(self._handle, name)

    def tracked_open(path: Path, *args, **kwargs):
        handle = original_open(path, *args, **kwargs)
        if path.resolve() == table_path and args and args[0] == "rb":
            return TrackingFile(handle)
        return handle

    original_parquet_file = result_store_module.pq.ParquetFile

    class FooterOnlyParquetFile:
        def __init__(self, *args, **kwargs) -> None:
            self._delegate = original_parquet_file(*args, **kwargs)

        @property
        def metadata(self):
            return self._delegate.metadata

        def read(self, *args, **kwargs):
            raise AssertionError("完整 Result 校验不得读取 Parquet 数据页")

    monkeypatch.setattr(result_store_module, "_HASH_CHUNK_BYTES", 32)
    monkeypatch.setattr(Path, "open", tracked_open)
    monkeypatch.setattr(
        result_store_module.pq,
        "ParquetFile",
        FooterOnlyParquetFile,
    )

    assert ResultStore(result_root, create=False).verify(directory) == bundle
    assert len(read_sizes) > 1
    assert set(read_sizes) == {32}


def test_result_bundle_rejects_cross_volume_staging_before_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_root, result_root, _ = _runtime_fixture(tmp_path)
    assembler = ResultAssembler(result_root)
    original_stat = Path.stat

    class StatWithDifferentDevice:
        def __init__(self, metadata) -> None:
            self._metadata = metadata
            self.st_dev = metadata.st_dev + 1

        def __getattr__(self, name: str):
            return getattr(self._metadata, name)

    def fake_stat(path: Path, *args, **kwargs):
        metadata = original_stat(path, *args, **kwargs)
        if path.parent == assembler.store.staging_root:
            return StatWithDifferentDevice(metadata)
        return metadata

    monkeypatch.setattr(Path, "stat", fake_stat)
    with pytest.raises(ResultContractError, match="不在同一卷"):
        assembler.finalize(
            run_root=run_root,
            project_id="research_package_test",
            package_hash=HASH_B,
            plan_hash=HASH_C,
            result_spec=_spec(),
            catalog_hashes={"prices": HASH_D},
            data_references={"prices": _data_reference()},
            metric_proofs=(_proof(),),
            implementation_manifest_hash=HASH_A,
            verification_policy_id=policy_id_for_claim("research_observation"),
            validity_producer_hash=VALIDITY_FACTS_PRODUCER_HASH,
        )
    namespace = result_root / "research_package_test" / HASH_A
    assert not namespace.exists() or not tuple(namespace.iterdir())


def test_result_spec_rejects_path_escape_and_multiple_primary() -> None:
    with pytest.raises(ResultContractError, match="POSIX"):
        ResultTableSpec(
            "bad", "primary", "statistics", "statistics", "data.columnar-bundle.v1",
            "data.columnar-bundle.metrics.v1", "../statistics",
        )
    primary = _spec().tables[0]
    with pytest.raises(ResultContractError, match="恰好"):
        ResultSpec.build((primary, ResultTableSpec(
            "second", "primary", "simulation", "simulation", "research.simulation.v1",
            "research.simulation.test.v1", "simulation",
        )))


def test_result_bundle_rejects_conflicting_second_finalize(tmp_path: Path) -> None:
    run_root, result_root, _, bundle, _, _ = _finalize(tmp_path)
    with pytest.raises(ResultContractError, match="另一份 canonical ResultBundle"):
        ResultAssembler(result_root).finalize(
            run_root=run_root,
            project_id=bundle.project_id,
            package_hash=HASH_D,
            plan_hash=bundle.plan_hash,
            result_spec=bundle.result_spec,
            catalog_hashes=bundle.catalog_hashes,
            data_references={"prices": _data_reference()},
            metric_proofs=bundle.metric_proofs,
            implementation_manifest_hash=bundle.implementation_manifest_hash,
            verification_policy_id=bundle.verification.policy_id,
            validity_producer_hash=bundle.verification.validity_producer_hash,
        )


@pytest.mark.parametrize("field", ["schema_hashes", "row_counts"])
def test_result_bundle_rejects_rehashed_table_manifest_drift(
    tmp_path: Path,
    field: str,
) -> None:
    run_root, result_root, _, _, directory, _ = _finalize(tmp_path)
    payload = json.loads((directory / "result.json").read_text(encoding="utf-8"))
    table = payload["tables"][0]
    relative_path = next(iter(table[field]))
    if field == "schema_hashes":
        table[field][relative_path] = "f" * 64
    else:
        table[field][relative_path] += 1
    table["table_manifest_hash"] = typed_canonical_hash({
        key: value for key, value in table.items() if key != "table_manifest_hash"
    })
    payload["result_id"] = typed_canonical_hash({
        key: value for key, value in payload.items() if key != "result_id"
    })
    (directory / "result.json").write_text(canonical_json(payload), encoding="utf-8")
    (directory / "COMMITTED").write_text(payload["result_id"], encoding="ascii")
    rewritten = directory.with_name(payload["result_id"])
    directory.rename(rewritten)
    with pytest.raises(ResultContractError, match="schema 或行数漂移"):
        ResultStore(result_root, create=False).verify(rewritten)


def test_result_survives_run_root_deletion_and_reads_each_parquet_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_root, result_root, _, bundle, directory, _ = _finalize(tmp_path)
    shutil.rmtree(run_root)
    table_path = directory / next(iter(bundle.tables[0].files))
    original_open = Path.open
    table_opens = 0

    def counted_open(path: Path, *args, **kwargs):
        nonlocal table_opens
        if path == table_path:
            table_opens += 1
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", counted_open)
    table = ResultStore(result_root, create=False).read_table_by_schema_id(
        bundle, schema_id="data.columnar-bundle.metrics.v1"
    )
    assert table["value"].to_pylist() == [1.25]
    assert table_opens == 1


def test_result_bundle_supports_two_nodes_and_two_public_table_roles(tmp_path: Path) -> None:
    run_root, result_root = _dual_table_runtime_fixture(tmp_path)
    result_spec = ResultSpec.build((
        ResultTableSpec(
            "diagnostics", "diagnostic", "diagnostics", "diagnostics",
            "research.diagnostics.v1", "research.diagnostics.test.v1", "diagnostics",
        ),
        ResultTableSpec(
            "primary_statistics", "primary", "statistics", "statistics",
            "data.columnar-bundle.v1", "data.columnar-bundle.metrics.v1", "statistics",
        ),
    ))
    bundle, directory, _ = ResultAssembler(result_root).finalize(
        run_root=run_root,
        project_id="research_package_test",
        package_hash=HASH_B,
        plan_hash=HASH_C,
        result_spec=result_spec,
        catalog_hashes={"prices": HASH_D},
        data_references={"prices": _data_reference()},
        metric_proofs=(_proof(),),
        implementation_manifest_hash=HASH_A,
        verification_policy_id=policy_id_for_claim("research_observation"),
        validity_producer_hash=VALIDITY_FACTS_PRODUCER_HASH,
    )
    assert tuple(table.table_id for table in bundle.tables) == (
        "diagnostics", "primary_statistics",
    )
    assert tuple(table.source_port for table in bundle.tables) == (
        "diagnostics", "statistics",
    )
    assert ResultStore(result_root, create=False).verify(directory) == bundle


def test_result_snapshot_does_not_open_unrequested_table(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_root, result_root = _dual_table_runtime_fixture(tmp_path)
    result_spec = ResultSpec.build((
        ResultTableSpec(
            "diagnostics", "diagnostic", "diagnostics", "diagnostics",
            "research.diagnostics.v1", "research.diagnostics.test.v1", "diagnostics",
        ),
        ResultTableSpec(
            "primary_statistics", "primary", "statistics", "statistics",
            "data.columnar-bundle.v1", "data.columnar-bundle.metrics.v1", "statistics",
        ),
    ))
    bundle, _directory, _ = ResultAssembler(result_root).finalize(
        run_root=run_root,
        project_id="research_package_test",
        package_hash=HASH_B,
        plan_hash=HASH_C,
        result_spec=result_spec,
        catalog_hashes={"prices": HASH_D},
        data_references={"prices": _data_reference()},
        metric_proofs=(_proof(),),
        implementation_manifest_hash=HASH_A,
        verification_policy_id=policy_id_for_claim("research_observation"),
        validity_producer_hash=VALIDITY_FACTS_PRODUCER_HASH,
    )
    diagnostic_manifest = next(
        item for item in bundle.tables if item.schema_id == "research.diagnostics.test.v1"
    )
    diagnostic_path = (
        ResultStore(result_root, create=False).result_directory(bundle)
        / next(iter(diagnostic_manifest.files))
    ).resolve()
    original_open = Path.open

    def reject_diagnostic_open(path: Path, *args, **kwargs):
        if path.resolve() == diagnostic_path:
            raise AssertionError("未请求的大表不得被打开")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", reject_diagnostic_open)
    snapshot = ResultStore(result_root, create=False).load_snapshot_by_identity(
        project_id=bundle.project_id,
        run_id=bundle.run_id,
        result_id=bundle.result_id,
        schema_ids=("data.columnar-bundle.metrics.v1",),
        verify_all_files=False,
    )

    assert tuple(snapshot.tables) == ("data.columnar-bundle.metrics.v1",)


def test_result_snapshot_verifies_lazy_table_without_reading_data_pages(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _run_root, result_root, _, bundle, _directory, _ = _finalize(tmp_path)
    original_parquet_file = result_store_module.pq.ParquetFile

    class FooterOnlyParquetFile:
        def __init__(self, *args, **kwargs) -> None:
            self._delegate = original_parquet_file(*args, **kwargs)

        @property
        def metadata(self):
            return self._delegate.metadata

        def read(self, *args, **kwargs):
            raise AssertionError("lazy schema 校验不得读取 Parquet 数据页")

    monkeypatch.setattr(
        result_store_module.pq,
        "ParquetFile",
        FooterOnlyParquetFile,
    )
    snapshot = ResultStore(result_root, create=False).load_snapshot_by_identity(
        project_id=bundle.project_id,
        run_id=bundle.run_id,
        result_id=bundle.result_id,
        verify_schema_ids=("data.columnar-bundle.metrics.v1",),
        verify_all_files=False,
    )

    assert not snapshot.tables
    assert snapshot.verified_schema_ids == frozenset({"data.columnar-bundle.metrics.v1"})
    with pytest.raises(ResultContractError, match="未进入本次已验证快照"):
        snapshot.table_schema("research.unrequested.test.v1")


@pytest.mark.parametrize(
    "changes",
    (
        {"value": float("nan")},
        {"value": float("inf")},
        {"sample_start": "not-a-date"},
        {"sample_start": "2026-07-01", "sample_end": "2026-06-30"},
        {"sample_start": "2026-01-01", "sample_end": "2026-06-30T00:00:00+00:00"},
        {"sample_size": 0},
        {"status": "estimated"},
    ),
)
def test_result_metric_decoder_rejects_invalid_facts(tmp_path: Path, changes) -> None:
    _run_root, _result_root, _object_root, bundle, directory, _reference = _finalize(
        tmp_path
    )
    row = {
        "metric_ref": "data.row_count@1.0.0",
        "value": 1.25,
        "unit": "rows",
        "sample_start": "2026-01-01",
        "sample_end": "2026-06-30",
        "sample_size": 120,
        "status": "computed",
    }
    row.update(changes)
    snapshot = ResultSnapshot(
        bundle,
        directory,
        {"data.columnar-bundle.metrics.v1": pa.Table.from_pylist([row])},
        {},
    )
    with pytest.raises(ResultContractError):
        metrics_from_snapshot(snapshot)


def test_result_metric_decoder_rejects_multiple_rows_for_one_metric(tmp_path: Path) -> None:
    _run_root, _result_root, _object_root, bundle, directory, _reference = _finalize(
        tmp_path
    )
    row = {
        "metric_ref": "data.row_count@1.0.0",
        "value": 1.25,
        "unit": "rows",
        "sample_start": "2026-01-01",
        "sample_end": "2026-06-30",
        "sample_size": 120,
        "status": "computed",
    }
    snapshot = ResultSnapshot(
        bundle,
        directory,
        {"data.columnar-bundle.metrics.v1": pa.Table.from_pylist([row, row])},
        {},
    )
    with pytest.raises(ResultContractError, match="恰好有一行"):
        metrics_from_snapshot(snapshot)
