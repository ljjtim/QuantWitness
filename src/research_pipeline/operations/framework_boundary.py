"""研究框架与项目扩展之间的静态边界门禁。"""

from __future__ import annotations

import ast
import hashlib
import json
import re
from datetime import date
from pathlib import Path
from typing import Iterable, Mapping

CONTRACT_VERSION = "research-framework-boundary-v2"

_DEFAULT_GRAPH_LOADERS = frozenset({
    "_load_graph_plan",
    "load_graph_plan",
    "load_operator_graph_plan",
})
_DEFAULT_SIBLING_LOOKUPS = frozenset({"_node", "find_node", "require_node"})
_DEFAULT_FIXED_IDENTITY_FIELDS = frozenset({
    "package_id",
    "research_id",
    "result_id",
    "reference_id",
})
_DEFAULT_PROJECT_STAGE_AGNOSTIC_CALLS = frozenset({
    "require_futures_d0_receipt",
})
_PROJECT_MODULE_MARKERS = (
    ".project_extensions.",
    ".research_packages.",
    ".retirement_migration.",
    "project_extensions.",
    "research_packages.",
    "retirement_migration.",
)
_RESULT_SCHEMA_ID = re.compile(
    r"^(?:research|data)\.[a-z0-9][a-z0-9_.-]*\.v[0-9]+$"
)
_RESULT_PATH = re.compile(
    r"^(?:evaluation|simulation|project|result|tables)/[A-Za-z0-9_./-]+$"
)
_RESULT_SEMANTIC_MODULE_SUFFIXES = frozenset({
    "research_pipeline/evidence/verification_result.py",
    "research_pipeline/evidence/result_financial_oracle.py",
})


def _issue(
    code: str,
    path: str,
    *,
    line: int | None,
    observed: str,
    remediation: str,
    **details: object,
) -> dict[str, object]:
    result: dict[str, object] = {
        "code": code,
        "path": path,
        "observed": observed,
        "remediation": remediation,
    }
    if line is not None:
        result["line"] = line
    result.update(details)
    return result


def _attribute_parts(node: ast.AST) -> tuple[str, ...]:
    parts: list[str] = []
    current = node
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if isinstance(current, ast.Name):
        parts.append(current.id)
    return tuple(reversed(parts))


def _call_name(node: ast.Call) -> str | None:
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return None


def _contains_name(node: ast.AST, name: str) -> bool:
    return any(
        isinstance(item, ast.Name) and item.id == name
        or isinstance(item, ast.Attribute) and item.attr == name
        for item in ast.walk(node)
    )


def _string_literals(node: ast.AST) -> tuple[str, ...]:
    return tuple(
        item.value
        for item in ast.walk(node)
        if isinstance(item, ast.Constant) and isinstance(item.value, str)
    )


def _fixed_values(node: ast.AST) -> tuple[str, ...]:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return (node.value,)
    if isinstance(node, (ast.Set, ast.List, ast.Tuple)):
        return tuple(
            item.value
            for item in node.elts
            if isinstance(item, ast.Constant) and isinstance(item.value, str)
        )
    return ()


def _keyword_values(node: ast.Call, name: str) -> tuple[str, ...]:
    for keyword in node.keywords:
        if keyword.arg == name:
            return _fixed_values(keyword.value)
    return ()


def _is_project_module(value: str) -> bool:
    lowered = value.lower()
    return any(marker in lowered for marker in _PROJECT_MODULE_MARKERS)


def _mode_comparison_literals(node: ast.AST) -> tuple[str, ...]:
    if not isinstance(node, ast.Compare):
        return ()
    operands = (node.left, *node.comparators)
    mode_operand = any(
        any(
            name == "mode" or name.endswith("_mode")
            for name in (
                item.id if isinstance(item, ast.Name) else item.attr
                for item in ast.walk(operand)
                if isinstance(item, (ast.Name, ast.Attribute))
            )
        )
        for operand in operands
    )
    if not mode_operand:
        return ()
    return tuple(
        literal
        for operand in operands
        for literal in _fixed_values(operand)
    )


def _semantic_field_names(nodes: Iterable[ast.AST]) -> frozenset[str]:
    names: set[str] = set()
    for node in nodes:
        for item in ast.walk(node):
            if isinstance(item, ast.Name):
                names.add(item.id.lower())
            elif isinstance(item, ast.Attribute):
                names.add(item.attr.lower())
            elif (
                isinstance(item, ast.Subscript)
                and isinstance(item.slice, ast.Constant)
                and isinstance(item.slice.value, str)
            ):
                names.add(item.slice.value.lower())
    return frozenset(names)


def _is_stage_field(name: str) -> bool:
    return name == "stage" or name.endswith("_stage") or "stage." in name


def _is_reason_field(name: str) -> bool:
    return name in {"reason", "reason_code"} or name.endswith("_reason")


def _identity_fields(node: ast.AST, fields: frozenset[str]) -> set[str]:
    observed: set[str] = set()
    for item in ast.walk(node):
        if isinstance(item, ast.Name) and item.id in fields:
            observed.add(item.id)
        elif isinstance(item, ast.Attribute) and item.attr in fields:
            observed.add(item.attr)
        elif (
            isinstance(item, ast.Subscript)
            and isinstance(item.slice, ast.Constant)
            and item.slice.value in fields
        ):
            observed.add(str(item.slice.value))
    return observed


def _iterated_plan_names(node: ast.AST) -> set[str]:
    if not isinstance(node, (ast.For, ast.comprehension)):
        return set()
    iterator = node.iter
    if not isinstance(iterator, ast.Call) or not isinstance(
        iterator.func, ast.Attribute
    ):
        return set()
    if iterator.func.attr not in {"items", "values"}:
        return set()
    owner = _attribute_parts(iterator.func.value)
    if not owner or owner[-1] != "admitted_plans":
        return set()
    target = node.target
    if iterator.func.attr == "items":
        if not isinstance(target, (ast.Tuple, ast.List)) or len(target.elts) < 2:
            return set()
        target = target.elts[1]
    return {item.id for item in ast.walk(target) if isinstance(item, ast.Name)}


def _discovers_dataset_role(node: ast.AST, plan_names: set[str]) -> bool:
    for item in ast.walk(node):
        parts = _attribute_parts(item)
        if (
            len(parts) >= 3
            and parts[0] in plan_names
            and parts[-2:] == ("query", "dataset_id")
        ):
            return True
    return False


def _structural_exemption_rules(
    policy: Mapping[str, object],
    path: str,
) -> frozenset[str]:
    result: set[str] = set()
    for item in policy.get("structural_exemptions", ()):
        if not isinstance(item, Mapping):
            continue
        declared = item.get("path")
        if not isinstance(declared, str) or not (
            declared == path or path.endswith("/" + declared)
        ):
            continue
        rules = item.get("rules")
        if isinstance(rules, list):
            result.update(str(rule) for rule in rules)
    return frozenset(result)


def _unwrap_literal_collection(node: ast.AST) -> ast.AST:
    if not (
        isinstance(node, ast.Call)
        and len(node.args) == 1
        and not node.keywords
    ):
        return node
    name = _call_name(node)
    if name not in {"MappingProxyType", "dict", "frozenset", "list", "set", "tuple"}:
        return node
    return _unwrap_literal_collection(node.args[0])


def _literal_field_names(node: ast.AST) -> frozenset[str] | None:
    value = _unwrap_literal_collection(node)
    if not isinstance(value, ast.Dict):
        return None
    names: set[str] = set()
    for key, item in zip(value.keys, value.values):
        if not (
            isinstance(key, ast.Constant)
            and isinstance(key.value, str)
            and isinstance(item, ast.Constant)
            and isinstance(item.value, str)
        ):
            return None
        names.add(key.value)
    return frozenset(names)


def _fixed_business_role_inventory(node: ast.AST) -> tuple[str, ...]:
    if not isinstance(node, (ast.Assign, ast.AnnAssign)):
        return ()
    value = _unwrap_literal_collection(node.value)
    if not isinstance(value, ast.Dict):
        return ()
    roles: list[str] = []
    fields: set[str] = set()
    for key, item in zip(value.keys, value.values):
        if not isinstance(key, ast.Constant) or not isinstance(key.value, str):
            return ()
        item_fields = _literal_field_names(item)
        if not item_fields:
            return ()
        roles.append(key.value)
        fields.update(item_fields)
    if len(roles) < 3 or not {"session", "code"} <= fields:
        return ()
    return tuple(sorted(roles))


def _structural_issues(
    tree: ast.AST,
    *,
    path: str,
    policy: Mapping[str, object],
) -> list[dict[str, object]]:
    rules = policy.get("structural_rules")
    rule_config = rules if isinstance(rules, Mapping) else {}
    graph_loaders = frozenset(
        str(item)
        for item in rule_config.get("graph_loader_calls", _DEFAULT_GRAPH_LOADERS)
    )
    sibling_lookups = frozenset(
        str(item)
        for item in rule_config.get(
            "sibling_node_lookup_calls", _DEFAULT_SIBLING_LOOKUPS
        )
    )
    fixed_identity_fields = frozenset(
        str(item)
        for item in rule_config.get(
            "fixed_identity_fields", _DEFAULT_FIXED_IDENTITY_FIELDS
        )
    )
    project_stage_agnostic_calls = frozenset(
        str(item)
        for item in rule_config.get(
            "project_stage_agnostic_calls",
            _DEFAULT_PROJECT_STAGE_AGNOSTIC_CALLS,
        )
    )
    exemptions = _structural_exemption_rules(policy, path)
    issues: list[dict[str, object]] = []
    seen: set[tuple[str, int]] = set()

    def append(issue: dict[str, object]) -> None:
        key = (str(issue["code"]), int(issue.get("line", 0)))
        if issue["code"] not in exemptions and key not in seen:
            seen.add(key)
            issues.append(issue)

    normalized_path = path.replace("\\", "/")
    if any(
        normalized_path.endswith(suffix)
        or normalized_path.endswith(f"/{Path(suffix).name}")
        for suffix in _RESULT_SEMANTIC_MODULE_SUFFIXES
    ):
        parent: dict[ast.AST, ast.AST] = {
            child: node
            for node in ast.walk(tree)
            for child in ast.iter_child_nodes(node)
        }
        allowed_lines = {
            item.lineno
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and _call_name(node) == "_ResultSemanticHandler"
            for item in ast.walk(node)
            if isinstance(item, ast.Constant) and isinstance(item.value, str)
        }
        for node in ast.walk(tree):
            if not (
                isinstance(node, ast.Constant)
                and isinstance(node.value, str)
                and (
                    _RESULT_SCHEMA_ID.fullmatch(node.value)
                    or _RESULT_PATH.fullmatch(node.value)
                )
                and node.lineno not in allowed_lines
            ):
                continue
            ancestors: list[ast.AST] = []
            current = parent.get(node)
            while current is not None:
                ancestors.append(current)
                current = parent.get(current)
            if any(
                isinstance(item, ast.Assign)
                and any(
                    isinstance(target, ast.Name)
                    and target.id.startswith("BUILTIN_RESULT_SCHEMA")
                    for target in item.targets
                )
                for item in ancestors
            ):
                continue
            inside_function = any(
                isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
                for item in ancestors
            )
            assignment_names = {
                target.id
                for item in ancestors
                if isinstance(item, ast.Assign)
                for target in item.targets
                if isinstance(target, ast.Name)
            }
            semantic_assignment = any(
                any(marker in name.upper() for marker in (
                    "SCHEMA", "PATH", "PREFIX", "RESULT",
                ))
                for name in assignment_names
            )
            if inside_function or semantic_assignment:
                append(_issue(
                    "unregistered_result_semantic_literal",
                    path,
                    line=node.lineno,
                    observed=(
                        "VerificationResult 执行代码在 handler 声明外固定解释 "
                        f"schema/path: {node.value}"
                    ),
                    remediation=(
                        "将固定 schema、路径、阶段和 verifier 身份登记到 "
                        "_ResultSemanticHandler，并从注册表投影执行参数"
                    ),
                ))

    for node in ast.walk(tree):
        role_inventory = _fixed_business_role_inventory(node)
        if role_inventory:
            append(_issue(
                "fixed_business_role_inventory",
                path,
                line=getattr(node, "lineno", None),
                observed=(
                    "生产 core 固定多业务角色及 session/code 字段集合: "
                    f"roles={list(role_inventory)}"
                ),
                remediation=(
                    "将项目输入角色留在 ResearchPackage/project extension；"
                    "公共原语只消费调用方声明的 typed schema，已晋级的内建合同需显式审查豁免"
                ),
            ))
        if isinstance(node, ast.Call):
            name = _call_name(node)
            call_values: tuple[ast.AST, ...] = (
                *node.args,
                *(item.value for item in node.keywords),
            )
            if (
                name in graph_loaders
                and any(_contains_name(item, "plan_root") for item in call_values)
                and any(_contains_name(item, "manifest") for item in call_values)
            ):
                append(_issue(
                    "business_full_graph_access",
                    path,
                    line=node.lineno,
                    observed=f"业务调用 {name} 同时读取 plan_root 与 manifest",
                    remediation="由 compiler/scheduler 解析完整图，业务节点只接收自己的参数和 typed inputs",
                ))
            if (
                name in sibling_lookups
                and len(node.args) >= 2
                and _string_literals(node.args[1])
            ):
                append(_issue(
                    "business_sibling_operator_lookup",
                    path,
                    line=node.lineno,
                    observed=f"调用 {name} 按固定身份查询图中其他节点",
                    remediation="通过 typed port 和当前节点参数传递依赖，不按兄弟 operator/node 身份查找",
                ))
            full_plan_values = [
                value
                for value in call_values
                if len(_attribute_parts(value)) >= 2
                and _attribute_parts(value)[-1] == "admitted_plans"
            ]
            if full_plan_values:
                append(_issue(
                    "business_full_admitted_plan_access",
                    path,
                    line=node.lineno,
                    observed="业务调用直接下传完整 runtime admitted_plans",
                    remediation="先按当前节点显式 request_id/角色投影计划子集，再交给业务实现",
                ))
            implementation_modules = (
                *_keyword_values(node, "module_name"),
                *_keyword_values(node, "dependency_modules"),
            )
            project_modules = tuple(
                value for value in implementation_modules
                if _is_project_module(value)
            )
            if project_modules:
                append(_issue(
                    "project_module_in_public_implementation",
                    path,
                    line=node.lineno,
                    observed=(
                        "公共实现声明包含项目模块: "
                        f"{sorted(project_modules)}"
                    ),
                    remediation="项目模块只进入当前项目组合，不得计入公共实现清单或摘要",
                ))
            if (
                name in project_stage_agnostic_calls
                and any(keyword.arg == "stage" for keyword in node.keywords)
            ):
                append(_issue(
                    "project_stage_routed_capability",
                    path,
                    line=node.lineno,
                    observed=f"通用能力调用 {name} 传入项目阶段 stage",
                    remediation="改为声明实际所需的数据、PIT 或交易规则能力，不按项目阶段路由",
                ))
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            docstring = (ast.get_docstring(node) or "").lower()
            if (
                "只供测试" in docstring
                or "test only" in docstring
                or "only for tests" in docstring
            ):
                append(_issue(
                    "test_only_helper_in_production",
                    path,
                    line=node.lineno,
                    observed=f"生产函数 {node.name} 明确声明只供测试使用",
                    remediation=(
                        "将测试 fixture/helper 移到 tests；"
                        "生产模板必须有真实生产消费者和独立合同"
                    ),
                ))
            argument_names = {
                argument.arg
                for argument in (
                    *node.args.posonlyargs,
                    *node.args.args,
                    *node.args.kwonlyargs,
                )
            }
            if (
                node.name in project_stage_agnostic_calls
                and "stage" in argument_names
            ):
                append(_issue(
                    "project_stage_routed_capability",
                    path,
                    line=node.lineno,
                    observed=f"通用能力 {node.name} 暴露项目阶段 stage 参数",
                    remediation="接口只声明实际所需的数据、PIT 或交易规则能力",
                ))
        elif isinstance(node, (ast.For, ast.comprehension)):
            plan_names = _iterated_plan_names(node)
            if plan_names and _discovers_dataset_role(node, plan_names):
                append(_issue(
                    "business_unbound_plan_discovery",
                    path,
                    line=getattr(node, "lineno", None),
                    observed="遍历完整 admitted_plans 并读取 plan.query.dataset_id 猜测输入角色",
                    remediation="在节点参数中声明 request_id 与角色的绑定，只读取该绑定对应的计划",
                ))
        elif isinstance(node, ast.Compare):
            operands = (node.left, *node.comparators)
            identity_fields = set().union(
                *(_identity_fields(item, fixed_identity_fields) for item in operands)
            )
            literals = {
                literal
                for index, operand in enumerate(operands)
                if not _identity_fields(operand, fixed_identity_fields)
                for literal in _fixed_values(operand)
            }
            if identity_fields and literals:
                append(_issue(
                    "fixed_public_identity",
                    path,
                    line=node.lineno,
                    observed=(
                        "公共代码按固定身份字符串分支: "
                        f"fields={sorted(identity_fields)}, literals={sorted(literals)}"
                    ),
                    remediation="改为声明式 registry/profile 或参数语义判断，不按 package/research/result/reference 固定身份分支",
                ))
        elif isinstance(node, ast.If):
            mode_literals = _mode_comparison_literals(node.test)
            if mode_literals:
                semantic_fields = _semantic_field_names(node.body)
                if (
                    any(_is_stage_field(name) for name in semantic_fields)
                    and any(_is_reason_field(name) for name in semantic_fields)
                ):
                    append(_issue(
                        "mode_bound_project_stage_reason",
                        path,
                        line=node.lineno,
                        observed=(
                            "固定 mode 分支同时解释 stage/reason 字段: "
                            f"modes={sorted(set(mode_literals))}"
                        ),
                        remediation=(
                            "core 只校验通用 envelope；项目阶段和 reason code "
                            "由当前项目 Verifier 解释"
                        ),
                    ))
    return issues


def _token_pattern(token: str) -> re.Pattern[str]:
    return re.compile(
        rf"(?<![A-Za-z0-9]){re.escape(token)}(?![A-Za-z0-9])"
    )


def _excluded(path: Path, source: Path, exclusions: Iterable[object]) -> bool:
    relative = path.relative_to(source)
    for raw in exclusions:
        excluded = Path(str(raw))
        if relative == excluded or excluded in relative.parents:
            return True
    return False


def audit_framework_boundary(
    repository_root: str | Path,
    *,
    source_root: str | Path | None = None,
    policy_path: str | Path | None = None,
) -> dict[str, object]:
    """检查核心源码是否依赖项目实现或硬编码项目合同。"""
    root = Path(repository_root).resolve()
    source = (root / "research_pipeline/src/research_pipeline" if source_root is None else Path(source_root)).resolve()
    policy_file = (root / "research_pipeline/release/framework-boundary/policy.json" if policy_path is None else Path(policy_path)).resolve()
    policy_bytes = policy_file.read_bytes()
    policy = json.loads(policy_bytes)
    issues: list[dict[str, object]] = []
    exemptions_used: list[dict[str, str]] = []
    today = date.today().isoformat()
    for item in policy.get("exemptions", []):
        if not isinstance(item, Mapping):
            continue
        if item.get("expires_at", "") < today:
            issues.append({"code": "exemption_expired", "path": item.get("path"), "token": item.get("token")})
    scan_paths = policy.get("scan_paths", ["."])
    exclude_paths = policy.get("exclude_paths", ())
    files: list[Path] = []
    for relative in scan_paths:
        raw = Path(str(relative))
        candidate = (root / raw if raw.parts and raw.parts[0] == "research_pipeline" else source / raw).resolve()
        try:
            candidate.relative_to(source)
        except ValueError:
            issues.append({"code": "scan_path_outside_source", "path": str(relative)})
            continue
        if candidate.is_file():
            if not _excluded(candidate, source, exclude_paths):
                files.append(candidate)
        elif candidate.is_dir():
            files.extend(sorted(
                p
                for p in candidate.rglob("*")
                if p.is_file()
                and p.suffix in {".py", ".yaml", ".yml", ".json", ".toml"}
                and not _excluded(p, source, exclude_paths)
            ))
    forbidden_imports = tuple(policy.get("forbidden_import_prefixes", ()))
    forbidden_tokens = tuple(policy.get("forbidden_tokens", ()))
    exemptions = list(policy.get("exemptions", ()))
    for path in sorted(set(files)):
        text = path.read_text(encoding="utf-8")
        rel = path.relative_to(root).as_posix()
        if path.suffix == ".py":
            try:
                tree = ast.parse(text)
            except SyntaxError as exc:
                issues.append({"code": "source_parse_error", "path": rel, "detail": str(exc)})
                continue
            for node in ast.walk(tree):
                targets: list[str] = []
                if isinstance(node, ast.Import):
                    targets = [a.name for a in node.names]
                elif isinstance(node, ast.ImportFrom):
                    targets = [node.module or ""]
                for target in targets:
                    if any(target == prefix or target.startswith(prefix + ".") for prefix in forbidden_imports):
                        issues.append(_issue(
                            "forbidden_import",
                            rel,
                            line=node.lineno,
                            observed=f"core 导入项目/退休模块: {target}",
                            remediation="将项目实现留在 project extension，core 只依赖通用合同",
                            target=target,
                        ))
            issues.extend(_structural_issues(tree, path=rel, policy=policy))
        for token in forbidden_tokens:
            if not _token_pattern(token).search(text):
                continue
            matching = [e for e in exemptions if e.get("path") == rel and e.get("token") == token and e.get("expires_at", "") >= today]
            if matching:
                exemptions_used.append({"path": rel, "token": token})
            else:
                issues.append(_issue(
                    "forbidden_token",
                    rel,
                    line=None,
                    observed=f"core 包含项目身份标识: {token}",
                    remediation="删除项目身份依赖，改为通用参数、typed port 或项目 extension",
                    token=token,
                ))
    source_hash = hashlib.sha256("\n".join(f"{p.relative_to(root).as_posix()}:{hashlib.sha256(p.read_bytes()).hexdigest()}" for p in sorted(set(files))).encode()).hexdigest()
    return {
        "contract_version": policy.get("contract_version", CONTRACT_VERSION),
        "status": "pass" if not issues else "fail",
        "policy_hash": hashlib.sha256(policy_bytes).hexdigest(),
        "source_hash": source_hash,
        "issues": issues,
        "exemptions_used": exemptions_used,
    }


__all__ = ["CONTRACT_VERSION", "audit_framework_boundary"]
