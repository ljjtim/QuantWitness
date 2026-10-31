"""在正式执行前用真实冻结参数检查项目 Worker ABI。"""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Mapping

from research_pipeline.platform.operator_contracts import OperatorGraphRecipe

from .errors import ExtensionError
from .project_bundle import ProjectOperatorContext


def preflight_project_operator_parameters(
    recipe: OperatorGraphRecipe,
    *,
    registry: object,
    fixed_clock: str,
    root_seed: int,
    timeout_seconds: int = 30,
) -> None:
    """对已登记钩子的项目节点调用一次纯参数预检。"""

    project_token = getattr(registry, "project_token", None)
    bundle_paths = getattr(registry, "bundle_paths", None)
    if not callable(project_token) or not isinstance(bundle_paths, Mapping):
        return
    for node in recipe.nodes:
        token = project_token(node.operator_id, node.operator_version)
        if token is None or token.manifest.parameter_preflight_module is None:
            continue
        bundle_path = Path(
            bundle_paths[token.implementation_id]
        ).resolve(strict=True)
        profile = token.manifest.operator_spec.resource_profile
        context = ProjectOperatorContext(
            project_id=token.manifest.project_id,
            run_id="parameter-preflight",
            node_id=node.node_id,
            attempt_id="parameter-preflight",
            fixed_clock=fixed_clock,
            root_seed=root_seed,
            parameters=(node.parameters if token.manifest.parameter_preflight_parameters is None
                        else {name: node.parameters[name]
                              for name in token.manifest.parameter_preflight_parameters
                              if name in node.parameters}),
            effective_resource_budget={
                key: int(profile[key])
                for key in ("memory_bytes", "cpu_slots", "temp_bytes", "wall_seconds")
            },
        )
        environment = dict(os.environ)
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        try:
            completed = subprocess.run(
                (
                    sys.executable,
                    "-m",
                    "research_pipeline.extensions.project_parameter_preflight_worker",
                    "--bundle",
                    str(bundle_path),
                ),
                input=json.dumps(context.to_dict(), ensure_ascii=False),
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                env=environment,
                timeout=timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise ExtensionError(
                f"项目算子参数预检超时: {node.node_id}",
                failure_payload={
                    "contract_version": "project-parameter-preflight-v1",
                    "node_id": node.node_id,
                    "error_code": "project_parameter_preflight_timeout",
                },
            ) from exc
        try:
            payload = json.loads(completed.stdout)
        except json.JSONDecodeError as exc:
            raise ExtensionError(
                f"项目算子参数预检没有返回有效结果: {node.node_id}",
                failure_payload={
                    "contract_version": "project-parameter-preflight-v1",
                    "node_id": node.node_id,
                    "error_code": "project_parameter_preflight_result_invalid",
                },
            ) from exc
        if (
            completed.returncode != 0
            or not isinstance(payload, dict)
            or payload.get("status") != "pass"
        ):
            error_code = (
                str(payload.get("error_code"))
                if isinstance(payload, dict)
                else "project_parameter_preflight_failed"
            )
            message = (
                str(payload.get("message"))
                if isinstance(payload, dict)
                else "项目参数解释失败"
            )
            raise ExtensionError(
                f"项目算子参数预检失败: {node.node_id}: {message}",
                failure_payload={
                    "contract_version": "project-parameter-preflight-v1",
                    "node_id": node.node_id,
                    "error_code": error_code,
                    "exception_type": (
                        payload.get("exception_type")
                        if isinstance(payload, dict)
                        else None
                    ),
                    "message": message,
                },
            )


__all__ = ["preflight_project_operator_parameters"]
