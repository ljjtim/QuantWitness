# 命令行

正式顶层命令：

```text
catalog, package, run, resume, retry-node, inspect, rerun-from,
verify, report, compare, export-result, doctor, gc, capabilities,
operator, artifact, recipe, workspace
```

以 `python -m research_pipeline --help` 和子命令 `--help` 为准。

## 发现与创建

```powershell
python -m research_pipeline capabilities --format json
python -m research_pipeline operator list --format json
python -m research_pipeline operator describe <operator-id> --format json
python -m research_pipeline artifact describe <artifact-type> --format json
python -m research_pipeline recipe list --format json
python -m research_pipeline catalog dataset search <关键词> --catalog-lock <持久Catalog-Lock目录> --format json
python -m research_pipeline package init <package目录> --json
```

QuantWitness 公开发行包不附带供应商或个人数据库的 Catalog。先用自己的声明和审批文件执行
`catalog validate/compile --definition ... --approvals ...` 生成持久 Catalog Lock，再显式传给
搜索、lint 和 admit。当前公共 Recipe 清单为空，以 `package init` 创建未填写研究事实的中性草稿，补齐四份声明后才能通过正式准入。Catalog dataset/field 搜索机器结果统一返回
`catalog_context`：其中的 `lock_reference` 是本次实际
加载且可直接传给 `--catalog-lock` 的稳定目录，`source_profiles` 列出正式 profile/environment。
dataset 命中项另含当前 `bindings`；`next_commands` 会带入 lock，并为每个命中 profile 保留显式
只读路径占位符。field 命中项不重复大块 binding 数据。

## lint 与准入

```powershell
python -m research_pipeline package lint --package <package目录> --catalog-lock <搜索结果.catalog_context.lock_reference> --json
python -m research_pipeline package admit --package <package目录> --catalog-lock <搜索结果.catalog_context.lock_reference> --data-db <source:prod只读DuckDB> --output <计划目录> --json
```

`package lint` 不打开研究数据库；草稿 lint 报出当前阻断字段，完整包 lint 返回编译结果、资源声明和准入缺口。`package admit` 只接收完整包，根据显式只读数据源自动生成漂移/PIT 闭包。

`workspace init` 同样创建中性草稿；`workspace validate` 检查工作区路径、四份 YAML 的结构与文件边界，不代替检查研究内容的 `package lint`。草稿补齐前不得运行 `package admit`。

多个数据源使用可重复的 `--source-db PROFILE=PATH`。来源声明为 `archived_snapshot` 时，lint、admit 和 package 消费命令必须提供 `--source-archive-root`。

直接采集的 raw 分钟查询没有额外来源版本参数。admit 从当前 Catalog binding 判断来源是否为
`raw + collected + pit_allowed`，并检查 completed bar、实际 schema、有界时区范围和稳定排序。
复权分钟或历史截面状态缺少各自 PIT 快照时仍拒绝。分钟 `AUDIT` 可用于只读观察，但若其输出
进入 Feature、Label、Signal、Statistics、Validity 或交易仿真，准入会明确拒绝。

## 项目算子

```powershell
python -m research_pipeline operator scaffold --output <新算子目录> --project-id <项目ID> --operator-id <算子ID> --format json
python -m research_pipeline operator validate --spec <operator.yaml> --source <源码目录> --format json
python -m research_pipeline operator build --spec <operator.yaml> --source <源码目录> --output <bundle父目录> --format json
python -m research_pipeline package lint --package <package目录> --extension-bundle <bundle目录> --json
python -m research_pipeline package admit --package <package目录> --extension-bundle <bundle目录> --catalog-lock <Catalog-Lock> --data-db <只读DuckDB> --output <计划目录> --json
```

scaffold 目标必须不存在，生成 `operator.yaml`、`source/operator.py` 和 README；声明由现行
`ProjectOperatorDeclaration` 序列化，不维护第二套 schema。入口是可由 Worker 以三个位置参数调用
的同步函数 `run(context, inputs, output_root)`。正式工件写入 `output_root`，返回每个声明端口对应的
typed commit；缺少 commit、端口/Artifact 不符、未声明文件或字节/hash 漂移均失败。

`--source` 是唯一源码闭包。CLI 不扫描当前目录、不自动安装依赖。bundle 在 admit 后进入计划闭包，run 和恢复不再接受 bundle 路径。

## 运行与恢复

```powershell
python -m research_pipeline run --plan <计划目录> --data-db <只读DuckDB> --artifact-root <工件目录> --handoff-out <handoff.json> --run-root <run目录> --result-store <ResultStore> --clock <带时区ISO时间> --root-seed 0 --json
python -m research_pipeline inspect --run-root <run目录> --json
python -m research_pipeline resume --run-root <run目录> --json
python -m research_pipeline retry-node --run-root <run目录> --node <node-id> --json
python -m research_pipeline rerun-from --run-root <父run目录> --output-run-root <子run目录> --node <node-id> --json
```

不提供资源参数时，单次 CLI 使用 16 GiB 总内存、当前进程可用的全部逻辑核心和 64 GiB scratch 上限。内部 worker 默认 1 个；显式 `--workers` 不得超过 CPU 或 `max_workers` 容量，项目 Worker 在节点总资源预算内自行管理任务并行。`--resource-memory-bytes`、`--resource-cpu-slots` 和 `--resource-scratch-bytes` 可以覆盖本次进程内容量。

只有同时运行多个独立 CLI 时才需要仓库外 `--resource-state-dir`，并额外提供 process 总额度和 timeout。它只等待或拒绝，不改变样本、分区、seed 或金融语义。

`inspect --json` 返回 Runtime/节点/attempt 状态、每个节点的重试用量与最后错误、当前身份下的
checkpoint 复验、Result finalize 独立状态，以及唯一 `recommended_action` 和 `next_command`。
只有 retry policy 允许且仍有余额的错误才建议 `retry-node`。需要人工修改 package 时，
`next_command` 只给可直接执行的 `package lint --help`，不会伪造仓库未保存的 package 路径。
finalize 失败但 `result_published=true` 时，`next_command` 使用真实 Result 路径进入 verify；该字段会实际改变下一步，不是只展示的回执。

## 结果验证与消费

```powershell
python -m research_pipeline verify --result <Result目录> --result-store <ResultStore> --output <VerificationResult.json> --verification-memory-bytes <字节> --verification-temp-bytes <字节> --verification-scratch-root <临时目录> --json
python -m research_pipeline report --verification-result <VerificationResult.json> --result-store <ResultStore> --json
python -m research_pipeline compare --left-verification-result <左.json> --left-result-store <左Store> --right-verification-result <右.json> --right-result-store <右Store> --json
python -m research_pipeline export-result --verification-result <VerificationResult.json> --result-store <ResultStore> --output <导出目录> --json
```

三个 `--verification-*` 参数只约束独立金融 oracle。默认进程预算为 1 GiB、临时盘预算为 8 GiB；未指定 scratch root 时使用系统临时目录。大型 canonical/TCA 使用批次扫描和受限 DuckDB，不按固定 Result 大小跳过；真实额度不足会使 `verify` 失败且不产生输出文件。

package 也提供绑定 ResearchPackage 的 report、compare 和 export-result 入口。直接 `compare`
返回 `verified_metric_facts_only`，明确说明未检查 package 合同；它只在已验证的指标单位、方向、
窗口、样本量、状态和实际 claim 事实一致时输出逐指标 delta。完整研究语义比较使用
`package compare`，由 `packages.delivery` 唯一检查两侧 metric/claim 合同并返回
`package_contract_and_verified_metric_facts`。不同 plan 只作为说明，不再单独否决比较。
`export-result` 只复制并复核已验证 Result；真正重跑仍使用 package → admit → run → verify。

## 运维

```powershell
python -m research_pipeline doctor --run-root <run目录> --json
python -m research_pipeline gc --root <工件根> --ttl-seconds 86400 --json
```

`gc` 默认只给计划；真正应用需要显式 `--apply`。
