"""个人 Research Workspace v1 的目录、配置和执行分配合同。

本模块只处理工作区元数据和路径安全，不读取研究数据库，也不负责运行
ResearchPackage。数据库路径只作为显式的机器绑定记录，交给现有主链在真正
运行时以只读方式打开。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import uuid
from typing import Any

import yaml

from .data_plane.path_policy import PathRolePolicy
from .packages import initialize_research_package


WORKSPACE_CONTRACT_VERSION = "research-workspace-v1"
WORKSPACE_FILE = "research-workspace.yaml"
LOCAL_FILE = ".research/local.yaml"
GENERATED_ROOT = ".research"
MANAGED_GITIGNORE_BEGIN = "# >>> research-workspace-v1 managed >>>"
MANAGED_GITIGNORE_END = "# <<< research-workspace-v1 managed <<<"
MANAGED_GITIGNORE_LINES = (
    "/.research/",
    "**/*.duckdb",
    "**/*.duckdb.*",
    "**/*.parquet",
    "**/*.key",
    "**/*.pem",
    "**/.env",
    "**/.env.*",
    "**/__pycache__/",
    "**/.ipynb_checkpoints/",
)
PACKAGE_FILES = {
    "package.yaml",
    "sources/sources.yaml",
    "localization.yaml",
    "spec/research.yaml",
    "README.md",
}
FORBIDDEN_PACKAGE_SUFFIXES = {
    ".py", ".pyc", ".sql", ".duckdb", ".db", ".sh", ".ps1", ".bat", ".ipynb"
}
COMPLETION_IDENTITY_KEYS = (
    "plan_hash",
    "package_plan_hash",
    "run_id",
    "runtime_run_id",
    "result_id",
    "evidence_hash",
    "evidence_semantic_hash",
)


class WorkspaceError(ValueError):
    """工作区合同或安全边界错误。"""


@dataclass(frozen=True)
class WorkspaceConfig:
    """已校验的工作区配置。"""

    root: Path
    workspace_id: str
    package_path: PurePosixPath
    operators_path: PurePosixPath
    generated_path: PurePosixPath
    database_path: str | None = None

    @property
    def package_root(self) -> Path:
        return self.root / Path(*self.package_path.parts)

    @property
    def operators_root(self) -> Path:
        return self.root / Path(*self.operators_path.parts)

    @property
    def generated_root(self) -> Path:
        return self.root / Path(*self.generated_path.parts)


def initialize_workspace(
    root: str | Path,
    *,
    workspace_id: str | None = None,
    allow_existing: bool = False,
) -> WorkspaceConfig:
    """在仓库外初始化工作区，不触碰数据库或数据文件。"""

    target = Path(root).expanduser().absolute()
    if target.exists():
        if not target.is_dir():
            raise WorkspaceError("工作区根路径必须是目录")
        if any(target.iterdir()) and not allow_existing:
            raise WorkspaceError("非空工作区默认失败；如确认安全请使用 --allow-existing")
    created_root = False
    if not target.exists():
        target.mkdir(parents=True)
        created_root = True

    workspace_id = _validate_workspace_id(workspace_id or target.name or "research-workspace")
    package_root = target / "package"
    if package_root.exists():
        raise WorkspaceError("package 目录已存在，拒绝覆盖")

    # 先完成普通目录和配置，再调用唯一的 ResearchPackage 模板来源。
    try:
        for relative in (
            "operators", "notebooks", "references", ".research/bundles",
            ".research/stores/artifacts", ".research/stores/results",
            ".research/dashboard",
        ):
            (target / relative).mkdir(parents=True, exist_ok=False)
        initialize_research_package(package_root)
        _write_text_exclusive(
            target / WORKSPACE_FILE,
            _yaml_text({
                "contract_version": WORKSPACE_CONTRACT_VERSION,
                "workspace_id": workspace_id,
                "package_path": "package",
                "operators_path": "operators",
                "generated_path": GENERATED_ROOT,
            }),
        )
        _write_text_exclusive(
            target / "README.md",
            "# Research Workspace\n\n机器生成内容统一放在 `.research/`，请使用 `research_pipeline workspace validate` 检查边界。\n",
        )
        _write_text_exclusive(target / ".research/index.json", _json_text(_empty_index(workspace_id)))
        _merge_gitignore(target / ".gitignore")
    except Exception:
        if created_root:
            shutil.rmtree(target, ignore_errors=True)
        raise
    return load_workspace(target)


def load_workspace(root: str | Path) -> WorkspaceConfig:
    """加载并校验工作区声明，不打开数据库路径。"""

    target = Path(root).expanduser().absolute()
    if not target.is_dir():
        raise WorkspaceError("工作区根目录不存在")
    config_path = target / WORKSPACE_FILE
    try:
        payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise WorkspaceError("无法读取工作区声明") from exc
    if not isinstance(payload, dict):
        raise WorkspaceError("工作区声明顶层必须是映射")
    expected = {"contract_version", "workspace_id", "package_path", "operators_path", "generated_path"}
    if set(payload) - expected or not expected.issubset(payload):
        raise WorkspaceError("工作区声明字段不完整或包含未知字段")
    if payload["contract_version"] != WORKSPACE_CONTRACT_VERSION:
        raise WorkspaceError("工作区 contract_version 不受支持")
    workspace_id = _validate_workspace_id(payload["workspace_id"])
    package_path = _safe_relative(payload["package_path"], "package_path")
    operators_path = _safe_relative(payload["operators_path"], "operators_path")
    generated_path = _safe_relative(payload["generated_path"], "generated_path")
    if len({package_path, operators_path, generated_path}) != 3:
        raise WorkspaceError("工作区路径角色不得相同")
    database_path = _load_local_database_path(target)
    config = WorkspaceConfig(target, workspace_id, package_path, operators_path, generated_path, database_path)
    _validate_layout(config)
    return config


def validate_workspace(root: str | Path) -> dict[str, Any]:
    """执行只读工作区检查，返回稳定的机器结果。"""

    config = load_workspace(root)
    required = {
        "package": config.package_root,
        "operators": config.operators_root,
        "generated": config.generated_root,
    }
    missing = [role for role, path in required.items() if not path.is_dir()]
    if missing:
        raise WorkspaceError(f"工作区目录缺失: {', '.join(missing)}")
    try:
        from .packages.store import validate_research_package_layout

        validate_research_package_layout(config.package_root)
    except Exception as exc:
        raise WorkspaceError(f"ResearchPackage 合同无效: {exc}") from exc
    forbidden_package = []
    for path in config.package_root.rglob("*"):
        if path.is_symlink():
            raise WorkspaceError("ResearchPackage 不允许符号链接")
        if path.is_file() and path.suffix.lower() in FORBIDDEN_PACKAGE_SUFFIXES:
            forbidden_package.append(path.relative_to(config.package_root).as_posix())
    if forbidden_package:
        raise WorkspaceError(f"ResearchPackage 含禁止文件: {', '.join(sorted(forbidden_package))}")
    git = _git_safety(config.root)
    return {
        "contract_version": WORKSPACE_CONTRACT_VERSION,
        "workspace_id": config.workspace_id,
        "root": str(config.root),
        "database_binding": {
            "configured": config.database_path is not None,
            "path": config.database_path,
            "opened": False,
        },
        "paths": {
            "package": str(config.package_root),
            "operators": str(config.operators_root),
            "generated": str(config.generated_root),
        },
        "git": git,
        "status": "fail" if git["status"] == "fail" else "pass",
    }


def allocate_execution(
    root: str | Path,
    *,
    clock: str,
    root_seed: int,
    label: str | None = None,
) -> dict[str, Any]:
    """分配一次不可复用的 execution 目录，不运行主链。"""

    config = load_workspace(root)
    _validate_clock(clock)
    if not isinstance(root_seed, int):
        raise WorkspaceError("root_seed 必须是整数")
    execution_id = "exec-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + "-" + uuid.uuid4().hex[:12]
    execution_root = config.generated_root / "executions" / execution_id
    execution_root.mkdir(parents=True)
    for name in ("plan", "run", "draft", "verification", "export-result"):
        (execution_root / name).mkdir()
    payload = {
        "contract_version": "research-workspace-execution-v1",
        "execution_id": execution_id,
        "workspace_id": config.workspace_id,
        "clock": clock,
        "root_seed": root_seed,
        "label": label,
        "status": "allocated",
        "database_opened": False,
    }
    _write_text_exclusive(execution_root / "execution.json", _json_text(payload))
    index_path = config.generated_root / "index.json"
    index = _read_index(index_path, config.workspace_id)
    index["executions"].append({
        "execution_id": execution_id,
        "clock": clock,
        "root_seed": root_seed,
        "label": label,
        "status": "allocated",
        "execution_path": execution_root.relative_to(config.root).as_posix(),
    })
    index["executions"].sort(key=lambda item: item["execution_id"])
    index["index_hash"] = _payload_hash(index, omit={"index_hash"})
    _atomic_replace_text(index_path, _json_text(index))
    return {**payload, "execution_path": str(execution_root), "index_path": str(index_path)}


def inspect_workspace(root: str | Path, execution_id: str | None = None) -> dict[str, Any]:
    """只读查看工作区索引或指定 execution。"""

    config = load_workspace(root)
    index = _read_index(config.generated_root / "index.json", config.workspace_id)
    if execution_id is not None:
        _, execution_root = _resolve_execution(config, index, execution_id)
        return _read_execution_declaration(execution_root, config.workspace_id)
    return index


def rebuild_workspace_index(root: str | Path) -> dict[str, Any]:
    """从 execution 声明和完成投影确定性重建导航索引。"""

    config = load_workspace(root)
    executions_root = config.generated_root / "executions"
    items: list[dict[str, Any]] = []
    if executions_root.is_dir():
        for execution_root in sorted(executions_root.iterdir(), key=lambda path: path.name):
            try:
                safe_execution_root = PathRolePolicy().resolve_contained_path(
                    allowed_root=executions_root,
                    candidate=execution_root,
                    root_role="executions_root",
                    path_role="execution_root",
                    expected_kind="directory",
                )
                allocation = _read_execution_declaration(
                    safe_execution_root, config.workspace_id
                )
            except Exception as exc:
                raise WorkspaceError(f"execution 目录无效: {execution_root.name}") from exc
            item = {
                "execution_id": safe_execution_root.name,
                "clock": allocation.get("clock"),
                "root_seed": allocation.get("root_seed"),
                "label": allocation.get("label"),
                "status": allocation.get("status", "allocated"),
                "execution_path": safe_execution_root.relative_to(config.root).as_posix(),
            }
            completion_path = safe_execution_root / "completion.json"
            if completion_path.is_file():
                completion = _read_completion(completion_path, safe_execution_root.name)
                item.update({key: value for key, value in completion.items() if key != "execution_id"})
            items.append(item)
    index = {
        "contract_version": "research-workspace-index-v1",
        "workspace_id": config.workspace_id,
        "executions": items,
    }
    index["index_hash"] = _payload_hash(index, omit={"index_hash"})
    _atomic_replace_text(config.generated_root / "index.json", _json_text(index))
    return index


def run_workspace_execution(
    root: str | Path,
    *,
    execution_id: str,
    plan: str | Path,
    data_db: str | Path,
    clock: str,
    root_seed: int,
    handler=None,
    **options: Any,
) -> dict[str, Any]:
    """把 Workspace execution 映射到现有 run 服务；本函数不猜测数据库路径。"""

    config = load_workspace(root)
    _validate_clock(clock)
    index = _read_index(config.generated_root / "index.json", config.workspace_id)
    item, execution_root = _resolve_execution(config, index, execution_id)
    if item.get("clock") != clock or item.get("root_seed") != root_seed:
        raise WorkspaceError("run 的 clock/root_seed 必须与 execution 声明完全一致")
    paths = {
        "artifact_root": execution_root / "artifacts",
        "handoff_out": execution_root / "handoff.json",
        "run_root": execution_root / "run",
        "result_store": execution_root / "results",
    }
    if handler is None:
        from .cli.commands.research_run import _execute as handler
    from argparse import Namespace

    payload = {
        "plan": str(Path(plan).resolve()),
        "data_db": str(Path(data_db).resolve()),
        "clock": clock,
        "root_seed": root_seed,
        "mode": options.pop("mode", "deterministic_serial"),
        "workers": options.pop("workers", None),
        "source_db": options.pop("source_db", []),
        "minute_data_root": options.pop("minute_data_root", None),
        "acceptance_proof": options.pop("acceptance_proof", None),
        **{key: str(value) for key, value in paths.items()},
        **options,
    }
    result = handler(Namespace(**payload))
    if not isinstance(result, dict):
        raise WorkspaceError("run 服务必须返回对象")
    _record_completion(config, index, item, execution_root, result)
    return {**result, "execution_id": execution_id, "execution_root": str(execution_root)}


def resume_workspace_execution(
    root: str | Path,
    *,
    execution_id: str,
    retry_node_id: str | None = None,
    handler=None,
) -> dict[str, Any]:
    """使用 execution 中现有 invocation/checkpoint 委托正式恢复服务。"""

    config = load_workspace(root)
    index = _read_index(config.generated_root / "index.json", config.workspace_id)
    item, execution_root = _resolve_execution(config, index, execution_id)
    run_root = execution_root / "run"
    invocation = run_root / "operator-dag-invocation.json"
    try:
        PathRolePolicy().resolve_contained_path(
            allowed_root=execution_root,
            candidate=invocation,
            root_role="execution_root",
            path_role="runtime_invocation",
            expected_kind="file",
        )
    except Exception as exc:
        raise WorkspaceError("execution 尚无可验证的正式 Runtime invocation") from exc
    if handler is None:
        from .cli.commands.research_run import resume_operator_graph as handler
    result = handler(run_root=run_root, retry_node_id=retry_node_id)
    if not isinstance(result, dict):
        raise WorkspaceError("恢复服务必须返回对象")
    _record_completion(config, index, item, execution_root, result)
    return {**result, "execution_id": execution_id, "execution_root": str(execution_root)}


def export_dashboard_manifest(root: str | Path) -> dict[str, Any]:
    """导出只含结构化 VerificationResult 的 Dashboard v3 清单。"""

    config = load_workspace(root)
    index = _read_index(config.generated_root / "index.json", config.workspace_id)
    manifest_path = config.generated_root / "workspace.json"
    from .evidence import EvidenceContractError, load_verified_result_context
    from .results import ResultContractError

    entries = []
    for item in index["executions"]:
        entry_id = str(item["execution_id"])
        _, execution_root = _resolve_execution(config, index, entry_id)
        verification_result = execution_root / "verification" / "result.json"
        result_store = execution_root / "results"
        if not verification_result.is_file():
            continue
        try:
            context = load_verified_result_context(
                verification_result,
                result_store=result_store,
            )
        except (EvidenceContractError, ResultContractError):
            continue
        if context.verification.status != "pass":
            continue
        reference = context.verification.result_reference
        if (
            reference.run_id != item.get("run_id")
            or reference.result_id != item.get("result_id")
        ):
            continue
        execution_rel = PurePosixPath(
            execution_root.relative_to(config.generated_root).as_posix()
        )
        entries.append({
            "entry_id": entry_id,
            "label": str(item.get("label") or entry_id),
            "verification_result": (
                execution_rel / "verification/result.json"
            ).as_posix(),
            "result_store": (execution_rel / "results").as_posix(),
        })
    if not entries:
        manifest_path.unlink(missing_ok=True)
        raise WorkspaceError(
            "没有与 execution 身份一致且 status=pass 的结构化 VerificationResult，"
            "拒绝生成 Dashboard 清单"
        )
    manifest = {
        "contract_version": "research-dashboard-workspace-v3",
        "entries": sorted(entries, key=lambda item: item["entry_id"]),
    }
    _atomic_replace_text(manifest_path, _json_text(manifest))
    return {
        "status": "pass",
        "manifest": str(manifest_path),
        "entry_count": len(entries),
    }


def _resolve_execution(
    config: WorkspaceConfig,
    index: dict[str, Any],
    execution_id: str,
) -> tuple[dict[str, Any], Path]:
    matches = [item for item in index["executions"] if item["execution_id"] == execution_id]
    if not matches:
        raise WorkspaceError("execution_id 不存在")
    item = matches[0]
    try:
        execution_path = _safe_relative(item.get("execution_path"), "execution_path")
        execution_root = PathRolePolicy().resolve_contained_path(
            allowed_root=config.generated_root,
            candidate=config.root / Path(*execution_path.parts),
            root_role="generated_root",
            path_role="execution_root",
            expected_kind="directory",
        )
    except Exception as exc:
        raise WorkspaceError("execution 路径越过 generated 目录") from exc
    allocation = _read_execution_declaration(execution_root, config.workspace_id)
    if allocation.get("execution_id") != execution_id:
        raise WorkspaceError("execution 声明身份不匹配")
    return item, execution_root


def _read_execution_declaration(execution_root: Path, workspace_id: str) -> dict[str, Any]:
    try:
        payload = json.loads((execution_root / "execution.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise WorkspaceError("execution 声明无法读取") from exc
    expected = {
        "contract_version", "execution_id", "workspace_id", "clock", "root_seed",
        "label", "status", "database_opened",
    }
    if (
        not isinstance(payload, dict)
        or set(payload) != expected
        or payload.get("contract_version") != "research-workspace-execution-v1"
        or payload.get("workspace_id") != workspace_id
        or payload.get("execution_id") != execution_root.name
        or isinstance(payload.get("root_seed"), bool)
        or not isinstance(payload.get("root_seed"), int)
        or payload.get("status") != "allocated"
        or not (payload.get("label") is None or isinstance(payload.get("label"), str))
        or payload.get("database_opened") is not False
    ):
        raise WorkspaceError("execution 声明身份不匹配")
    _validate_clock(payload["clock"])
    return payload


def _record_completion(
    config: WorkspaceConfig,
    index: dict[str, Any],
    item: dict[str, Any],
    execution_root: Path,
    result: dict[str, Any],
) -> None:
    completion_path = execution_root / "completion.json"
    completion = {"execution_id": item["execution_id"]}
    if completion_path.is_file():
        existing = _read_completion(completion_path, item["execution_id"])
        completion.update(existing)
    for key in COMPLETION_IDENTITY_KEYS:
        if key in result:
            if key in completion and completion[key] != result[key]:
                raise WorkspaceError(f"completion 不可变身份发生漂移: {key}")
            item[key] = result[key]
            completion[key] = result[key]
    completion["status"] = str(
        result.get("status", result.get("evidence_state", "completed"))
    )
    item["status"] = completion["status"]
    item["execution_path"] = execution_root.relative_to(config.root).as_posix()
    _atomic_replace_text(completion_path, _json_text(completion))
    index["index_hash"] = _payload_hash(index, omit={"index_hash"})
    _atomic_replace_text(config.generated_root / "index.json", _json_text(index))


def _read_completion(path: Path, execution_id: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise WorkspaceError("completion 投影无法读取") from exc
    allowed = {"execution_id", "status", *COMPLETION_IDENTITY_KEYS}
    if (
        not isinstance(payload, dict)
        or set(payload) - allowed
        or payload.get("execution_id") != execution_id
        or not isinstance(payload.get("status"), str)
    ):
        raise WorkspaceError("completion 投影身份不匹配")
    return payload


def _validate_layout(config: WorkspaceConfig) -> None:
    role_paths = {
        "package": config.package_root,
        "operators": config.operators_root,
        "generated": config.generated_root,
    }
    existing = {role: path for role, path in role_paths.items() if path.exists()}
    if existing:
        try:
            PathRolePolicy().validate(existing)
        except Exception as exc:
            raise WorkspaceError(f"工作区路径角色冲突: {exc}") from exc
    for role, path in role_paths.items():
        if path.exists() and path.is_symlink():
            raise WorkspaceError(f"路径角色 {role} 不允许符号链接")
    gitignore = config.root / ".gitignore"
    if not gitignore.is_file():
        raise WorkspaceError("缺少项目级 .gitignore")
    text = gitignore.read_text(encoding="utf-8")
    if text.count(MANAGED_GITIGNORE_BEGIN) != 1 or text.count(MANAGED_GITIGNORE_END) != 1:
        raise WorkspaceError(".gitignore 缺少或重复 Workspace 托管区块")
    managed = text.split(MANAGED_GITIGNORE_BEGIN, 1)[1].split(
        MANAGED_GITIGNORE_END, 1
    )[0]
    if tuple(line.strip() for line in managed.splitlines() if line.strip()) != MANAGED_GITIGNORE_LINES:
        raise WorkspaceError(".gitignore Workspace 托管区块缺少危险文件保护规则")


def _load_local_database_path(root: Path) -> str | None:
    path = root / LOCAL_FILE
    if not path.exists():
        return None
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise WorkspaceError("无法读取机器本地配置") from exc
    if not isinstance(payload, dict):
        raise WorkspaceError("机器本地配置必须是映射")
    value = payload.get("database_path")
    if value is None:
        return None
    if not isinstance(value, str) or not Path(value).is_absolute():
        raise WorkspaceError("database_path 必须是显式绝对路径")
    database = Path(value).expanduser().absolute()
    try:
        PathRolePolicy().validate(
            {"workspace_root": root, "database_input": database},
            read_only_roles=("database_input",),
        )
    except Exception as exc:
        raise WorkspaceError(f"database_path 路径边界无效: {exc}") from exc
    if not database.is_file():
        raise WorkspaceError("database_path 必须是已存在文件")
    return str(database)


def _git_safety(root: Path) -> dict[str, Any]:
    try:
        tracked = _git_lines(root, ("ls-files",))
        staged = _git_lines(root, ("diff", "--cached", "--name-only"))
        git_repo = True
    except WorkspaceError:
        return {"repository": False, "tracked_dangerous": [], "staged_dangerous": [], "status": "not_a_git_repository"}
    def is_dangerous(item: str) -> bool:
        normalized = item.replace("\\", "/").lower()
        dangerous = (
            ".duckdb", ".wal", ".parquet", ".key", ".pem", ".env",
            "token", "secret", "__pycache__", ".ipynb_checkpoints",
        )
        return normalized == GENERATED_ROOT or normalized.startswith(
            f"{GENERATED_ROOT}/"
        ) or any(part in normalized for part in dangerous)
    tracked_dangerous = sorted(item for item in tracked if is_dangerous(item))
    staged_dangerous = sorted(item for item in staged if is_dangerous(item))
    return {
        "repository": git_repo,
        "tracked_dangerous": tracked_dangerous,
        "staged_dangerous": staged_dangerous,
        "status": "fail" if tracked_dangerous or staged_dangerous else "pass",
    }


def _git_lines(root: Path, args: tuple[str, ...]) -> list[str]:
    result = subprocess.run(("git", "-C", str(root), *args), capture_output=True, text=True, check=False)
    if result.returncode:
        raise WorkspaceError("工作区不是 Git 仓库")
    return [line for line in result.stdout.splitlines() if line]


def _merge_gitignore(path: Path) -> None:
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    if MANAGED_GITIGNORE_BEGIN in existing:
        before = existing.split(MANAGED_GITIGNORE_BEGIN, 1)[0]
        after = existing.split(MANAGED_GITIGNORE_END, 1)[1]
        existing = before.rstrip() + "\n\n" + after.lstrip()
    block = "\n".join((MANAGED_GITIGNORE_BEGIN, *MANAGED_GITIGNORE_LINES, MANAGED_GITIGNORE_END))
    path.write_text((existing.rstrip() + "\n\n" + block + "\n").lstrip(), encoding="utf-8")


def _read_index(path: Path, workspace_id: str) -> dict[str, Any]:
    if not path.is_file():
        raise WorkspaceError("工作区 index.json 缺失")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise WorkspaceError("工作区 index.json 无法读取") from exc
    if (
        not isinstance(payload, dict)
        or set(payload) != {"contract_version", "workspace_id", "executions", "index_hash"}
        or payload.get("contract_version") != "research-workspace-index-v1"
        or payload.get("workspace_id") != workspace_id
        or not isinstance(payload.get("executions"), list)
    ):
        raise WorkspaceError("工作区 index.json 合同无效")
    execution_ids = []
    for item in payload["executions"]:
        if not isinstance(item, dict) or not isinstance(item.get("execution_id"), str):
            raise WorkspaceError("工作区 index.json execution 投影无效")
        execution_ids.append(item["execution_id"])
    if len(execution_ids) != len(set(execution_ids)) or execution_ids != sorted(execution_ids):
        raise WorkspaceError("工作区 index.json execution 必须唯一且有序")
    expected_hash = _payload_hash(payload, omit={"index_hash"})
    if payload.get("index_hash") != expected_hash:
        raise WorkspaceError("工作区 index.json 身份摘要不匹配")
    return payload


def _empty_index(workspace_id: str) -> dict[str, Any]:
    payload = {"contract_version": "research-workspace-index-v1", "workspace_id": workspace_id, "executions": []}
    payload["index_hash"] = _payload_hash(payload, omit={"index_hash"})
    return payload


def _payload_hash(payload: dict[str, Any], *, omit: set[str]) -> str:
    canonical = {key: value for key, value in payload.items() if key not in omit}
    return hashlib.sha256(json.dumps(canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _safe_relative(value: object, field: str) -> PurePosixPath:
    if not isinstance(value, str) or not value or "\\" in value:
        raise WorkspaceError(f"{field} 必须是安全 POSIX 相对路径")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts) or ":" in path.parts[0]:
        raise WorkspaceError(f"{field} 必须是安全 POSIX 相对路径")
    return path


def _validate_workspace_id(value: object) -> str:
    if not isinstance(value, str) or not value or len(value) > 80 or any(ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for ch in value):
        raise WorkspaceError("workspace_id 只能包含字母、数字、下划线和连字符")
    return value


def _validate_clock(value: object) -> None:
    if not isinstance(value, str):
        raise WorkspaceError("clock 必须是带时区的 ISO 时间")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise WorkspaceError("clock 不是有效 ISO 时间") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise WorkspaceError("clock 必须是带时区的 ISO 时间")


def _json_text(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n"


def _yaml_text(value: object) -> str:
    return yaml.safe_dump(value, allow_unicode=True, sort_keys=False)


def _write_text_exclusive(path: Path, content: str) -> None:
    if path.exists():
        raise WorkspaceError(f"目标文件已存在，拒绝覆盖: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8", newline="\n") as handle:
        handle.write(content)


def _atomic_replace_text(path: Path, content: str) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
        temporary.replace(path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


__all__ = [
    "WorkspaceConfig", "WorkspaceError", "allocate_execution", "initialize_workspace",
    "inspect_workspace", "load_workspace", "rebuild_workspace_index", "run_workspace_execution",
    "resume_workspace_execution",
    "export_dashboard_manifest", "validate_workspace",
]
