"""研究主链唯一命令面。"""

from __future__ import annotations

import argparse

from research_pipeline import __version__


DESCRIPTION = "QuantWitness 量化研究主链；数据源只读，研究产物写入显式目录。"
FINAL_COMMANDS = (
    "catalog", "package", "run", "resume", "retry-node", "inspect",
    "rerun-from", "verify", "report", "compare", "analysis", "export-result", "doctor", "gc",
    "capabilities",
    "operator", "artifact", "recipe",
    "workspace",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="quantwitness", description=DESCRIPTION)
    parser.add_argument(
        "--version", action="version", version=f"QuantWitness {__version__}"
    )
    commands = parser.add_subparsers(dest="command")
    _add_catalog(commands)
    _add_package(commands)
    _add_run(commands)
    _add_runtime(commands)
    _add_evidence(commands)
    _add_analysis(commands)
    _add_operations(commands)
    _add_capabilities(commands)
    _add_machine_discovery(commands)
    _add_workspace(commands)
    return parser


def _add_output_arguments(command: argparse.ArgumentParser) -> None:
    output = command.add_mutually_exclusive_group()
    output.add_argument("--json", action="store_true", help="完整机器结果")
    output.add_argument("--summary", action="store_true", help="精简机器摘要、错误与下一步命令")


def _add_catalog(commands: argparse._SubParsersAction) -> None:
    catalog = commands.add_parser("catalog", help="编译、检查和发布数据目录")
    subcommands = catalog.add_subparsers(dest="catalog_command", required=True)
    discover = subcommands.add_parser("discover", help="只读发现物理 schema")
    discover.add_argument("--source-kind", choices=("duckdb", "parquet"), required=True)
    discover.add_argument("--source", required=True)
    discover.add_argument("--object", required=True)
    discover.add_argument("--profile", required=True)
    discover.add_argument("--environment", required=True)
    discover.add_argument("--output", required=True)
    _add_output_arguments(discover)
    validate = subcommands.add_parser("validate", help="校验目录声明")
    validate.add_argument("--definition", action="append", required=True)
    validate.add_argument("--approvals")
    _add_output_arguments(validate)
    compile_command = subcommands.add_parser("compile", help="编译不可变目录锁")
    compile_command.add_argument("--definition", action="append", required=True)
    compile_command.add_argument("--approvals")
    compile_command.add_argument("--output", required=True)
    _add_output_arguments(compile_command)
    drift = subcommands.add_parser("drift", help="只读检查当前 schema 漂移")
    drift.add_argument("--release", required=True)
    drift.add_argument("--source-kind", choices=("duckdb", "parquet"), required=True)
    drift.add_argument("--source", required=True)
    drift.add_argument("--dataset-id", required=True)
    drift.add_argument("--dataset-version", type=int, default=1)
    drift.add_argument("--profile", required=True)
    drift.add_argument("--environment", required=True)
    drift.add_argument("--binding-version", type=int, default=1)
    _add_output_arguments(drift)
    docs = subcommands.add_parser("docs", help="从 lock 与 audit 生成文档")
    docs.add_argument("--release", required=True)
    docs.add_argument("--output", required=True)
    _add_output_arguments(docs)
    for kind in ("dataset", "field"):
        resource = subcommands.add_parser(kind, help=f"只读搜索 Catalog {kind}")
        resource_commands = resource.add_subparsers(
            dest="catalog_search_command",
            required=True,
        )
        search = resource_commands.add_parser("search", help=f"搜索 {kind}")
        search.add_argument("query")
        search.add_argument("--catalog-lock", required=True)
        search.add_argument("--format", choices=("json", "text"), default="text")


def _add_package(commands: argparse._SubParsersAction) -> None:
    package = commands.add_parser("package", help="声明式 ResearchPackage")
    subcommands = package.add_subparsers(dest="package_command", required=True)
    init = subcommands.add_parser("init", help="从安装包模板初始化")
    init.add_argument("destination")
    _add_output_arguments(init)
    variants = subcommands.add_parser(
        "expand-variants",
        help="从基包和受限参数清单原子生成多个完整 ResearchPackage",
    )
    variants.add_argument("--base", required=True)
    variants.add_argument("--manifest", required=True)
    variants.add_argument("--output-root", required=True)
    _add_output_arguments(variants)
    source_import = subcommands.add_parser("source-import", help="从本地文件离线归档来源正文")
    source_import.add_argument("--package", required=True)
    source_import.add_argument("--source-id", required=True)
    source_import.add_argument("--input-root", required=True)
    source_import.add_argument("--input-file", required=True)
    source_import.add_argument("--archive-root", required=True)
    source_import.add_argument("--media-type", required=True, help="例如 application/pdf")
    source_import.add_argument("--importer-id", default="research_pipeline.local_import")
    source_import.add_argument("--imported-at", help="可选：带时区 ISO 时间；省略则记录当前 UTC 时间")
    _add_output_arguments(source_import)
    lint = subcommands.add_parser("lint", help="无证书检查 package、字段、本地算子、指标和结果合同")
    lint.add_argument("--package", required=True)
    lint.add_argument("--catalog-lock", help="可选：提供后检查 dataset/version/field 引用")
    lint.add_argument("--source-archive-root", help="含 archived_snapshot 时必须提供")
    lint.add_argument("--extension-bundle", action="append", default=[], help="显式项目算子 bundle；可重复")
    lint.add_argument("--verifier-bundle", help="显式项目 Verifier bundle")
    _add_output_arguments(lint)
    admit = subcommands.add_parser("admit", help="复验正式材料并发布不可变运行计划")
    _add_admission_arguments(admit)
    for name in ("report", "export-result"):
        help_text = (
            "按 ResearchPackage 复制并复核已验证 Result；不重新执行研究"
            if name == "export-result"
            else "按 ResearchPackage 生成可信证据报告"
        )
        command = subcommands.add_parser(name, help=help_text)
        command.add_argument("--package", required=True)
        command.add_argument("--source-archive-root", help="含 archived_snapshot 时必须提供")
        command.add_argument("--verification-result", required=True)
        command.add_argument("--result-store", required=True)
        command.add_argument("--extension-bundle", action="append", default=[], help="显式项目算子 bundle；可重复")
        command.add_argument("--verifier-bundle", help="显式项目 Verifier bundle")
        if name == "export-result":
            command.add_argument("--output", required=True)
        else:
            command.add_argument("--output")
            command.add_argument(
                "--format", choices=("markdown", "json", "html"), default="markdown"
            )
            command.add_argument("--request", help="Qlib HTML 报告 JSON/YAML 请求；HTML 必填")
        _add_output_arguments(command)
    compare = subcommands.add_parser("compare", help="按同一合同比较可信证据")
    compare.add_argument("--package", required=True)
    compare.add_argument("--right-package")
    compare.add_argument("--source-archive-root", help="左侧 package 含 archived_snapshot 时必须提供")
    compare.add_argument("--right-source-archive-root", help="右侧 package 的来源归档根；省略时复用左侧")
    compare.add_argument("--left-verification-result", required=True)
    compare.add_argument("--right-verification-result", required=True)
    compare.add_argument("--left-result-store", required=True)
    compare.add_argument("--right-result-store", required=True)
    compare.add_argument("--extension-bundle", action="append", default=[], help="显式项目算子 bundle；可重复")
    compare.add_argument("--verifier-bundle", help="显式项目 Verifier bundle")
    _add_output_arguments(compare)


def _add_admission_arguments(
    command: argparse.ArgumentParser,
    *,
    required: bool = True,
) -> None:
    """正式 package admit 的显式只读输入。"""
    command.add_argument("--package", required=required)
    command.add_argument("--catalog-lock")
    command.add_argument("--data-db", help="source profile 的显式只读 DuckDB")
    command.add_argument("--input-snapshot-manifest", help="显式封存输入清单；与数据库来源互斥")
    command.add_argument(
        "--source-db",
        action="append",
        default=[],
        metavar="PROFILE=PATH",
        help="其他 Catalog source_profile 的显式只读 DuckDB；可重复",
    )
    command.add_argument("--source-archive-root", help="含 archived_snapshot 时必须提供")
    command.add_argument("--extension-bundle", action="append", default=[], help="显式项目算子 bundle；可重复")
    command.add_argument("--verifier-bundle", help="显式项目 Verifier bundle")
    command.add_argument("--output", help="正式 admit 必需：必须不存在的计划目录")
    _add_output_arguments(command)


def _add_run(commands: argparse._SubParsersAction) -> None:
    run = commands.add_parser("run", help="只读取数并执行完整合同 DAG")
    run.add_argument("--plan", required=True, help="package admit 生成的计划目录")
    run.add_argument("--data-db", help="显式只读 DuckDB")
    run.add_argument("--input-snapshot-manifest", help="显式封存输入清单；与数据库来源互斥")
    run.add_argument(
        "--minute-data-root",
        help="分钟算子图必需：显式只读 Parquet 根目录（含 stock/fund/index/futures）",
    )
    run.add_argument(
        "--source-db",
        action="append",
        default=[],
        metavar="PROFILE=PATH",
        help="为 Catalog source_profile 显式绑定额外只读 DuckDB；可重复传入",
    )
    run.add_argument("--artifact-root", required=True)
    run.add_argument("--handoff-out", required=True, help="数据平面 handoff 输出")
    run.add_argument("--run-root", required=True)
    _add_run_reuse_arguments(run)
    run.add_argument("--result-store", required=True, help="自包含 ResultStore")
    run.add_argument("--mode", choices=("deterministic_serial", "bounded_parallel", "partitioned_batch"), default="deterministic_serial")
    _add_runtime_resource_arguments(run)
    run.add_argument("--root-seed", type=int, default=0)
    run.add_argument("--clock", required=True, help="显式带时区的固定 ISO 时钟")
    _add_output_arguments(run)


def _add_runtime(commands: argparse._SubParsersAction) -> None:
    for name in ("resume", "retry-node"):
        command = commands.add_parser(name, help=f"统一运行时 {name}")
        command.add_argument("--run-root", required=True)
        if name == "retry-node":
            command.add_argument("--node", required=True)
        _add_output_arguments(command)
    inspect = commands.add_parser("inspect", help="检查统一运行时状态")
    inspect.add_argument("--run-root", required=True)
    _add_output_arguments(inspect)
    rerun = commands.add_parser(
        "rerun-from",
        help="从正式父运行的指定节点创建独立 child run",
    )
    rerun.add_argument("--run-root", required=True, help="父 run root")
    rerun.add_argument("--output-run-root", required=True)
    rerun.add_argument("--node", required=True)
    _add_output_arguments(rerun)


def _add_run_reuse_arguments(command: argparse.ArgumentParser) -> None:
    command.add_argument(
        "--reuse-run-root",
        action="append",
        default=[],
        help="显式复用已成功发布 Result 的完成态 run，可重复提供并按顺序尝试",
    )
    command.add_argument(
        "--require-reused-node",
        action="append",
        default=[],
        metavar="NODE_ID",
        help=(
            "要求指定节点必须从 --reuse-run-root 或 --reuse-failed-run-root 复用；可重复提供。"
            "Runtime 会在任何节点启动前预检，缺少一个就拒绝运行"
        ),
    )
    command.add_argument(
        "--reuse-failed-run-root",
        help=(
            "显式复用一个终态失败 run 中已成功且完整复验通过的 checkpoint；"
            "默认要求新旧 DAG 与环境完全一致；配合 --require-reused-node 可改变其余 DAG，指定节点及上游身份必须一致"
        ),
    )


def _add_runtime_resource_arguments(command: argparse.ArgumentParser) -> None:
    command.add_argument(
        "--workers",
        type=int,
        help="内部 worker 数；默认 1，显式指定时不能超过 CPU 或 max_workers 容量",
    )
    command.add_argument(
        "--resource-state-dir",
        help="仓库外的单机跨进程资源治理状态目录；不提供时只用进程内容量",
    )
    command.add_argument(
        "--resource-memory-bytes",
        type=int,
        help="本次 CLI 总内存上限（字节）；默认 16 GiB",
    )
    command.add_argument(
        "--resource-cpu-slots",
        type=int,
        help="本次 CLI 总 CPU 槽位；默认当前进程可用逻辑核心数",
    )
    command.add_argument(
        "--resource-scratch-bytes",
        type=int,
        help="本次 CLI scratch/temp 磁盘上限（字节）；默认 64 GiB，不预留空间",
    )
    command.add_argument(
        "--resource-process-slots",
        type=int,
        help="显式跨进程治理的总进程槽位",
    )
    command.add_argument(
        "--resource-timeout-seconds",
        type=float,
        help="显式跨进程 FIFO 资源申请超时秒数",
    )
    command.add_argument(
        "--resource-stale-seconds",
        type=float,
        default=30.0,
        help="显式跨进程租约 heartbeat 失效秒数，默认 30",
    )


def _add_verification_resource_arguments(command: argparse.ArgumentParser) -> None:
    command.add_argument(
        "--verification-memory-bytes",
        type=int,
        help="独立复核进程 memory 配额；省略时使用 1 GiB",
    )
    command.add_argument(
        "--verification-temp-bytes",
        type=int,
        help="独立复核 temp/spill 配额；省略时使用 8 GiB",
    )
    command.add_argument(
        "--verification-process-slots",
        type=int,
        default=2,
        help="独立复核进程槽，包含父进程与 Worker 树，默认 2",
    )
    command.add_argument(
        "--verification-scratch-root",
        help="已存在的独立复核临时目录根；省略时使用系统临时目录",
    )


def _add_evidence(commands: argparse._SubParsersAction) -> None:
    verify = commands.add_parser("verify", help="独立复核 Result 并生成 VerificationResult")
    verify.add_argument("--result", required=True, help="run 生成的自包含 Result 目录")
    verify.add_argument("--result-store", required=True)
    verify.add_argument("--output", required=True, help="必须不存在的 VerificationResult JSON")
    _add_verification_resource_arguments(verify)
    verify.add_argument("--verifier-bundle", help="与 Result 冻结身份一致的项目 Verifier bundle")
    _add_output_arguments(verify)
    for name in ("report", "export-result"):
        help_text = (
            "复制并复核已验证 Result；不重新执行研究"
            if name == "export-result"
            else "消费结构化 VerificationResult 生成报告"
        )
        command = commands.add_parser(name, help=help_text)
        command.add_argument("--verification-result", required=True)
        command.add_argument("--result-store", required=True)
        if name == "export-result":
            command.add_argument("--output", required=True)
        else:
            command.add_argument("--output")
            command.add_argument(
                "--format", choices=("markdown", "json", "html"), default="markdown"
            )
            command.add_argument("--request", help="Qlib HTML 报告 JSON/YAML 请求；HTML 必填")
        _add_output_arguments(command)
    compare = commands.add_parser("compare", help="比较两份结构化 VerificationResult")
    compare.add_argument("--left-verification-result", required=True)
    compare.add_argument("--right-verification-result", required=True)
    compare.add_argument("--left-result-store", required=True)
    compare.add_argument("--right-result-store", required=True)
    _add_output_arguments(compare)


def _add_analysis(commands: argparse._SubParsersAction) -> None:
    analysis = commands.add_parser(
        "analysis",
        help="只读分析已验证 Result 的显式时间序列",
    )
    subcommands = analysis.add_subparsers(dest="analysis_command", required=True)
    run = subcommands.add_parser("run", help="生成独立 AnalysisResult")
    run.add_argument("--verification-result", required=True)
    run.add_argument("--result-store", required=True)
    run.add_argument("--request", required=True, help="显式 AnalysisRequest YAML/JSON")
    run.add_argument("--output", required=True, help="必须不存在的 AnalysisResult JSON")
    run.add_argument(
        "--analysis-memory-bytes",
        type=int,
        default=8 * 1024 * 1024,
        help="日期和值两个投影列的批次预算；默认 8 MiB",
    )
    _add_output_arguments(run)

    compare = subcommands.add_parser(
        "compare",
        help="比较两份或多份同口径 AnalysisResult",
    )
    compare.add_argument("--analysis-result", action="append", required=True)
    compare.add_argument(
        "--measure",
        choices=(
            "overall.mean",
            "overall.absolute_change",
            "overall.compounded_return",
            "overall.geometric_annualized_return",
            "overall.max_drawdown",
        ),
        required=True,
    )
    compare.add_argument(
        "--direction",
        choices=("higher_is_better", "lower_is_better"),
        required=True,
    )
    compare.add_argument(
        "--output",
        required=True,
        help="必须不存在的 AnalysisComparison JSON",
    )
    _add_output_arguments(compare)


def _add_operations(commands: argparse._SubParsersAction) -> None:
    doctor = commands.add_parser("doctor", help="只读检查当前 Runtime run 和扩展注册")
    doctor.add_argument("--run-root", action="append", default=[])
    _add_output_arguments(doctor)
    gc = commands.add_parser("gc", help="研究工件垃圾回收；默认 dry-run")
    gc.add_argument("--root", required=True)
    gc.add_argument("--ttl-seconds", type=int, required=True)
    gc.add_argument("--now-ns", type=int)
    gc.add_argument("--apply", action="store_true")
    _add_output_arguments(gc)


def _add_capabilities(commands: argparse._SubParsersAction) -> None:
    capability = commands.add_parser("capabilities", help="只读输出机器能力清单")
    capability.add_argument("--format", choices=("text", "json"), default="text")


def _add_machine_discovery(commands: argparse._SubParsersAction) -> None:
    operator = commands.add_parser("operator", help="从正式 OperatorDefinition 清单发现算子")
    operator_commands = operator.add_subparsers(dest="operator_command", required=True)
    operator_list = operator_commands.add_parser("list")
    operator_list.add_argument("--format", choices=("json", "text"), default="text")
    operator_describe = operator_commands.add_parser("describe")
    operator_describe.add_argument("operator_id")
    operator_describe.add_argument("--format", choices=("json", "text"), default="text")
    operator_scaffold = operator_commands.add_parser(
        "scaffold",
        help="生成可原样 validate/build 的最小项目算子目录",
    )
    operator_scaffold.add_argument(
        "--output",
        required=True,
        help="必须不存在的新项目算子目录",
    )
    operator_scaffold.add_argument("--project-id", required=True)
    operator_scaffold.add_argument("--operator-id", required=True)
    operator_scaffold.add_argument("--operator-version", default="1.0.0")
    operator_scaffold.add_argument("--format", choices=("json", "text"), default="text")
    operator_validate = operator_commands.add_parser(
        "validate",
        help="复验项目 operator 薄声明和显式源码闭包",
    )
    operator_validate.add_argument("--spec", required=True, help="项目 operator YAML/JSON 薄声明")
    operator_validate.add_argument("--source", help="Python 项目算子的显式源码目录；框架绑定不接受")
    operator_validate.add_argument("--format", choices=("json", "text"), default="text")
    operator_build = operator_commands.add_parser(
        "build",
        help="生成内容寻址的项目 operator bundle",
    )
    operator_build.add_argument("--spec", required=True, help="项目 operator YAML/JSON 薄声明")
    operator_build.add_argument("--source", help="Python 项目算子的显式源码目录；框架绑定不接受")
    operator_build.add_argument("--output", required=True, help="bundle 内容寻址目录的父目录")
    operator_build.add_argument("--format", choices=("json", "text"), default="text")

    artifact = commands.add_parser("artifact", help="从算子端口合同发现 Artifact")
    artifact_commands = artifact.add_subparsers(dest="artifact_command", required=True)
    artifact_describe = artifact_commands.add_parser("describe")
    artifact_describe.add_argument("artifact_type")
    artifact_describe.add_argument("--format", choices=("json", "text"), default="text")

    recipe = commands.add_parser("recipe", help="从正式 Recipe registry 发现研究骨架")
    recipe_commands = recipe.add_subparsers(dest="recipe_command", required=True)
    recipe_list = recipe_commands.add_parser("list")
    recipe_list.add_argument("--format", choices=("json", "text"), default="text")
    recipe_describe = recipe_commands.add_parser("describe")
    recipe_describe.add_argument("recipe_id")
    recipe_describe.add_argument("--format", choices=("json", "text"), default="text")
    recipe_scaffold = recipe_commands.add_parser("scaffold")
    recipe_scaffold.add_argument("recipe_id")
    recipe_scaffold.add_argument("--output", help="必须不存在的新 ResearchPackage 目录")
    recipe_scaffold.add_argument(
        "--catalog-lock",
        required=True,
        help="显式持久 Catalog Lock，用于冻结 recipe 的数据集版本",
    )
    recipe_scaffold.add_argument("--answers", help="JSON 对象文件；与 --set 合并，--set 优先")
    recipe_scaffold.add_argument("--set", action="append", default=[], metavar="KEY=VALUE", help="结构化回答；VALUE 可为 JSON")
    recipe_scaffold.add_argument("--format", choices=("json", "text"), default="text")


def _add_workspace(commands: argparse._SubParsersAction) -> None:
    workspace = commands.add_parser("workspace", help="个人 Research Workspace v1 编排与 Git 隔离")
    subcommands = workspace.add_subparsers(dest="workspace_command", required=True)
    init = subcommands.add_parser("init", help="初始化仓库外个人工作区")
    init.add_argument("root")
    init.add_argument("--workspace-id")
    init.add_argument("--allow-existing", action="store_true")
    init.add_argument("--from-package", help="复制已填写的四份研究声明；目标必须不存在")
    _add_output_arguments(init)
    validate = subcommands.add_parser("validate", help="只读检查工作区合同与 Git 危险文件")
    validate.add_argument("--workspace", required=True)
    _add_output_arguments(validate)
    allocate = subcommands.add_parser("allocate", help="分配不可复用 execution 目录")
    allocate.add_argument("--workspace", required=True)
    allocate.add_argument("--clock", required=True)
    allocate.add_argument("--root-seed", type=int, required=True)
    allocate.add_argument("--label")
    _add_output_arguments(allocate)
    inspect = subcommands.add_parser("inspect", help="只读查看工作区索引或 execution")
    inspect.add_argument("--workspace", required=True)
    inspect.add_argument("--execution")
    _add_output_arguments(inspect)
    rebuild = subcommands.add_parser("rebuild-index", help="从 execution 投影重建导航索引")
    rebuild.add_argument("--workspace", required=True)
    _add_output_arguments(rebuild)
    run = subcommands.add_parser("run", help="把 execution 委托给现有 Research Pipeline run 服务")
    run.add_argument("--workspace", required=True)
    run.add_argument("--execution", required=True)
    _add_run_reuse_arguments(run)
    run.add_argument("--plan", required=True)
    run.add_argument("--data-db")
    run.add_argument("--input-snapshot-manifest", help="显式封存输入清单")
    run.add_argument("--clock", required=True)
    run.add_argument("--root-seed", type=int, required=True)
    run.add_argument("--mode", choices=("deterministic_serial", "bounded_parallel", "partitioned_batch"), default="deterministic_serial")
    _add_runtime_resource_arguments(run)
    run.add_argument("--source-db", action="append", default=[])
    run.add_argument("--minute-data-root")
    _add_output_arguments(run)
    resume = subcommands.add_parser("resume", help="复用 execution 中已验证的 invocation/checkpoint")
    resume.add_argument("--workspace", required=True)
    resume.add_argument("--execution", required=True)
    _add_output_arguments(resume)
    retry = subcommands.add_parser("retry-node", help="委托正式 Runtime 重试 execution 节点")
    retry.add_argument("--workspace", required=True)
    retry.add_argument("--execution", required=True)
    retry.add_argument("--node", required=True)
    _add_output_arguments(retry)
    execute = subcommands.add_parser("execute", help="依次检查、准入、运行、验证并报告工作区研究")
    execute.add_argument("--workspace", required=True)
    execute.add_argument("--catalog-lock", required=True)
    execute.add_argument("--data-db", help="显式只读 DuckDB")
    execute.add_argument("--input-snapshot-manifest", help="显式封存输入清单；与数据库来源互斥")
    execute.add_argument("--source-db", action="append", default=[])
    execute.add_argument("--source-archive-root")
    execute.add_argument("--extension-bundle", action="append", default=[])
    execute.add_argument("--verifier-bundle")
    execute.add_argument("--clock", help="省略时使用研究包；显式值必须与研究包一致")
    execute.add_argument("--root-seed", type=int, help="省略时使用研究包；显式值必须一致")
    execute.add_argument("--label")
    execute.add_argument("--minute-data-root")
    execute.add_argument("--mode", choices=("deterministic_serial", "bounded_parallel", "partitioned_batch"), default="deterministic_serial")
    _add_runtime_resource_arguments(execute)
    _add_run_reuse_arguments(execute)
    _add_verification_resource_arguments(execute)
    _add_output_arguments(execute)
    dashboard = subcommands.add_parser(
        "dashboard",
        help="导出 VerificationResult 与自包含 Result 的 Dashboard v3 清单",
        description="导出 VerificationResult 与自包含 Result 的 Dashboard v3 清单",
    )
    dashboard.add_argument("--workspace", required=True)
    _add_output_arguments(dashboard)
__all__ = ["DESCRIPTION", "FINAL_COMMANDS", "build_parser"]
