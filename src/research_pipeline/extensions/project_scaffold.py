"""从现行项目算子声明合同生成最小可运行 scaffold。"""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import shutil
import tempfile
from typing import Iterable

import yaml

from research_pipeline.platform.operator_contracts import OperatorSpec, PortSpec

from .errors import ExtensionError
from .governance import validate_project_operator_artifacts
from .project_bundle import (
    ProjectOperatorDeclaration,
    ProjectOperatorPermissionProfile,
    load_project_operator_declaration,
    project_source_hash,
)


PROJECT_OPERATOR_SCAFFOLD_VERSION = "project-operator-scaffold-v1"
_SCAFFOLD_PORT = "validity"
_SCAFFOLD_ARTIFACT_TYPE = "research.validity-facts.v1"
_SOURCE = '''"""项目算子最小 identity 示例；正式使用前替换为研究算法并补齐贴身测试。"""

import hashlib


def run(context, inputs, output_root):
    """复制唯一 typed input，并提交声明端口对应的真实工件。"""
    if len(inputs) != 1:
        raise ValueError("identity 算子要求恰好一个输入")
    source = inputs[0]
    if source.port != "validity" or source.artifact_type != "research.validity-facts.v1":
        raise ValueError("identity 算子输入端口或 Artifact 类型不符")
    source_path = output_root.parent / source.relative_path
    target = output_root / "validity" / "content.bin"
    target.parent.mkdir(parents=True)
    digest = hashlib.sha256()
    with source_path.open("rb") as reader, target.open("wb") as writer:
        for block in iter(lambda: reader.read(65536), b""):
            writer.write(block)
            digest.update(block)
    return {
        "port": "validity",
        "artifact_type": "research.validity-facts.v1",
        "relative_path": "validity/content.bin",
        "content_hash": digest.hexdigest(),
        "schema_hash": source.schema_hash,
        "byte_size": target.stat().st_size,
    }
'''


@dataclass(frozen=True)
class ProjectOperatorScaffold:
    root: Path
    declaration_path: Path
    source_root: Path
    readme_path: Path
    declaration: ProjectOperatorDeclaration
    contract_version: str = PROJECT_OPERATOR_SCAFFOLD_VERSION

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_version": self.contract_version,
            "root": str(self.root),
            "declaration_path": str(self.declaration_path),
            "source_root": str(self.source_root),
            "readme_path": str(self.readme_path),
            "project_id": self.declaration.project_id,
            "operator_id": self.declaration.operator_spec.operator_id,
            "operator_version": self.declaration.operator_spec.operator_version,
            "input_ports": [
                item.to_dict() for item in self.declaration.operator_spec.input_ports
            ],
            "output_ports": [
                item.to_dict() for item in self.declaration.operator_spec.output_ports
            ],
        }


def scaffold_project_operator(
    destination: str | Path,
    *,
    project_id: str,
    operator_id: str,
    operator_version: str,
    registered_operator_specs: Iterable[OperatorSpec],
) -> ProjectOperatorScaffold:
    """原子创建由现行 declaration dataclass 序列化的项目算子目录。"""

    target = Path(destination).resolve()
    if target.exists():
        raise ExtensionError(
            f"项目算子 scaffold 目标已存在: {target}",
            failure_payload={
                "contract_version": "project-operator-repair-v1",
                "code": "scaffold_target_exists",
                "missing_requirements": ["new_output_directory"],
                "next_commands": [],
            },
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.", dir=target.parent))
    try:
        source_root = staging / "source"
        source_root.mkdir()
        (source_root / "operator.py").write_text(_SOURCE, encoding="utf-8")
        specification = OperatorSpec.build(
            operator_id=operator_id,
            operator_version=operator_version,
            input_ports=(PortSpec(_SCAFFOLD_PORT, _SCAFFOLD_ARTIFACT_TYPE),),
            output_ports=(PortSpec(_SCAFFOLD_PORT, _SCAFFOLD_ARTIFACT_TYPE),),
            parameters=(),
            strategy_roles=(),
            resource_profile={
                "memory_bytes": 268_435_456,
                "cpu_slots": 1,
                "temp_bytes": 67_108_864,
                "wall_seconds": 30,
            },
            determinism_mode="deterministic",
            seed_policy="none",
            code_hash=project_source_hash(source_root),
            pit_capabilities=("pit.as_of.v1",),
        )
        validate_project_operator_artifacts(
            specification,
            registered_operator_specs,
        )
        declaration = ProjectOperatorDeclaration(
            project_id=project_id,
            operator_spec=specification,
            entry_module="operator",
            entry_function="run",
            dependency_lock={},
            permissions=ProjectOperatorPermissionProfile(),
        )
        declaration_path = staging / "operator.yaml"
        declaration_path.write_text(
            yaml.safe_dump(
                declaration.to_dict(),
                allow_unicode=True,
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        reloaded = load_project_operator_declaration(
            declaration_path,
            source_root=source_root,
        )
        if reloaded.to_dict() != declaration.to_dict():
            raise ExtensionError("项目算子 scaffold 声明未能按现行 schema 往返")
        (staging / "README.md").write_text(
            _readme(project_id, operator_id, operator_version),
            encoding="utf-8",
        )
        os.replace(staging, target)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return ProjectOperatorScaffold(
        root=target,
        declaration_path=target / "operator.yaml",
        source_root=target / "source",
        readme_path=target / "README.md",
        declaration=declaration,
    )


def _readme(project_id: str, operator_id: str, operator_version: str) -> str:
    return f"""# 项目算子 scaffold

此目录由现行 `ProjectOperatorDeclaration` 合同生成，可原样 validate/build。
当前实现是确定性的 typed identity，用于演示 Worker ABI；正式研究必须替换算法并补齐
手算 oracle、PIT、标签隔离、时间边界、空/重复/缺失、确定性和负控制测试。

- project：`{project_id}`
- operator：`{operator_id}@{operator_version}`
- 声明：`operator.yaml`
- 唯一源码闭包：`source/`

```powershell
python -m research_pipeline operator validate --spec operator.yaml --source source --format json
python -m research_pipeline operator build --spec operator.yaml --source source --output <bundle父目录> --format json
```

`source/` 只能放 Python 文件。正式输出必须写在 Worker 传入的 `output_root` 中，并返回与
声明端口、Artifact 类型、相对路径和真实字节一致的 commit；本 identity 示例不能替代具体
研究算法或研究证据。

多文件表格输入使用 `input.iter_batches(columns=..., batch_size=...)`；
`output_root.write_batches(...)` 与 `write_state(chunks=...)` 提供单批 32 MiB 上限，
父进程只接收 staging 提交描述。正式 Feature/Label 还需声明必填 JSON 参数
`causal_plan`，由 package 冻结键批、来源与时间窗口，再由核心生成逐行时间事实；
扩展只写键和值。完整合同见框架 `project_extensions/README.md`。
"""


__all__ = [
    "PROJECT_OPERATOR_SCAFFOLD_VERSION",
    "ProjectOperatorScaffold",
    "scaffold_project_operator",
]
