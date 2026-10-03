# AI 从研究问题到结果的工作协议

## 唯一状态机

```text
需求冻结
  → 能力和数据发现
  → 创建 ResearchPackage
  → package lint
  → package admit
  → run / inspect / resume / retry-node / rerun-from
  → Result
  → verify
  → VerificationResult
  → report / compare / export-result / Dashboard
```

AI 不创建项目 runner，不手写内部 drift bundle，不猜 Catalog、PIT 或计划 hash，也不把 `local_only` 写成已经发布。

## 每个阶段的决策

### 需求冻结

一次性整理研究口径。仍缺用户事实时，列出缺口和建议；不会改变研究含义的默认值才可自动填入。

### 发现

使用 `capabilities`、`operator`、`artifact`、`recipe` 和 `catalog`。Catalog 搜索结果中的
`catalog_context.lock_reference` 可直接交给 lint/admit，dataset 的 `bindings` 与
`catalog_context.source_profiles` 说明各 profile/environment 需要的只读路径；AI 不自行猜路径。
输出只是发现信息，不是运行资格。

### 算子选择

1. 同语义算子已存在：复用。
2. 当前研究需要的新算法：项目 extension。
3. 已有两个异构真实消费者、独立 oracle、前视/标签/边界攻击测试且合同无项目语义：人工评估晋级 core。

只有第 3 类才进入唯一 OperatorDefinition 清单；不自动评分，不保留旧 ID 转发。

### lint

研究包能完成加载和编译后，lint 返回声明、字段、算子图、ResultSpec、指标、来源、资源预算、准入缺口和下一条命令。中性草稿的独立静态缺口由 `data.issues` 聚合返回，每项包含 `code`、`file`、`field`、`message` 和 `action`；无法解析的文件不产生依赖于其内容的后续结论。`data.status=linted` 与 `data.execution_ready=false` 表示完成静态检查，仍需执行 admit。lint 失败就修改 package 或 bundle，不进入数据运行。

### admit

只提供当前 package、Catalog Lock、显式只读数据源和新输出目录。admit 自动构造漂移/PIT 证明和计划内 bundle 闭包。数据不可证明时保持 NOT_RUN。

直接采集的 raw 分钟查询无需额外来源版本清单。AI 必须让 admit 校验 completed bar、当前
Catalog 的 `raw + collected + pit_allowed` 语义、实际 schema、有界时区范围和稳定排序；
Runtime 再记录本次实际来源与读取内容身份。复权分钟和历史截面状态缺 PIT 快照时仍不进入
run。分钟 `AUDIT` 只能做扫描、重采样等观察，不能流入 Feature、Label、Signal、Statistics、
Validity 或交易仿真。

### run 与恢复

run 固定 plan、clock、seed、mode 和资源额度。恢复先看 `inspect` 的唯一建议：中断用 resume；只有错误码受 retry policy 允许且仍有余额时用 retry-node；终态分叉用 rerun-from；身份变化或 Result/package 合同失败则人工修改 package、重新 lint/admit 并新建 run。Runtime succeeded 后还要看独立 finalize 状态；旧 run 显示 `unknown`，不能据此声称已有 Result。

### verify 与消费

Result 是自包含计算结果；VerificationResult 是独立复验结论。报告、比较、导出和 Dashboard 必须同时绑定二者，不能只看 Runtime 的 succeeded。真正复现仍从 package lint/admit → run → verify 重新执行研究。

## 读取命令结果

操作型命令使用 `--summary` 获取精简 JSON，或用 `--json` 获取完整数据，两者互斥；发现命令按各自帮助使用 `--format json`。摘要中的 `command_status`、`execution_status`、`verification_status` 各自表示命令、执行和验证状态；不适用时为 null。摘要不读取数据库或重新验证 Result。

| 阶段 | 需要读取的事实 | 可以采取的下一步 |
| --- | --- | --- |
| 操作型命令返回 | 退出码、顶层 `status`、`error_code`、`message` | 非零退出码或顶层 `fail` 时处理错误；顶层 `pass` 只表示命令完成 |
| `package lint` | `data.status`、`data.execution_ready`、`data.checks`、`data.missing_requirements` | 根据准入缺口准备显式输入，再执行 admit |
| `run` 或恢复返回 | `data.status=result_finalized`、`data.result_id`、`data.result_directory` | 保存本次返回的结果位置，再独立 verify |
| `inspect` | `data.run_status`、`data.finalize.status`、`data.recommended_action`、`data.required_inputs` | 使用返回的恢复建议；仅节点成功不代表 Result 已封存 |
| `verify` | `data.status`、`data.validity_status`、`data.claim_level`、`data.output` | 验证结论为 `pass` 后，才把它作为通过验证的研究结果消费；`fail` 时阅读验证报告定位原因 |

`verify` 可以成功生成一份结论为 `fail` 的 VerificationResult，所以退出码 0 和顶层 `status=pass` 都不能替代 `data.status`。资源不足或命令异常也不等于研究金融结论为假，应先读错误原因。计算和独立验证各自证明什么，见 [证据与金融口径](evidence.md)。

自动执行建议命令时优先使用 `next_command_argv` 参数数组，不把路径拼接为 shell 字符串。命令建议中的显式占位参数仍需填入；缺少参数时不能猜测数据库位置、时点或样本。

Workspace 运行失败返回的 `data.failed_node`、`data.root_error` 和 `data.run_root` 用于定位节点错误；其他阶段的错误不保证带这些字段。新的 PowerShell 会话先用 `workspace inspect` 找到 execution；resume 或 retry-node 成功后，用本次返回值更新结果引用，不复用旧 `$run`。完整例子见 [Workspace 入门](workspace-quickstart.md)。

## 复用已有研究

第一次使用从公开源码中的完整示例开始，按 [Workspace 入门](workspace-quickstart.md)构建项目 bundle，用 `workspace init --from-package` 复制四份声明，再以 `workspace execute` 完成整条流程。`package init` 生成的是待填写草稿；`operator scaffold` 生成的是接口示例，两者都不替用户决定研究假设。

接入自己的数据时，先确定数据集、字段、可见时间、研究窗口和评价指标，再选择或编写对应项目算子。保存显式 Catalog Lock、只读数据路径、bundle、clock 和 seed；恢复直接使用既有 execution 的调用记录，不重新猜一组参数。数据源、研究口径或计划发生变化时，重新 lint/admit 并建立新运行。

资源不足时保留样本、频率、参数和 seed。根据诊断调整明确授权的预算；Windows venv 的独立验证示例使用 `--verification-process-slots 3`，具体来源见 [资源预算](project_resource_budgets.md)。

## 项目算子最短闭环

```powershell
python -m research_pipeline operator scaffold --output <新算子目录> --project-id <项目ID> --operator-id <算子ID> --format json
python -m research_pipeline operator validate --spec <operator.yaml> --source <源码目录> --format json
python -m research_pipeline operator build --spec <operator.yaml> --source <源码目录> --output <bundle父目录> --format json
python -m research_pipeline package lint --package <包目录> --extension-bundle <bundle目录> --json
python -m research_pipeline package admit --package <包目录> --extension-bundle <bundle目录> --catalog-lock <Catalog-Lock> --data-db <只读DuckDB> --output <计划目录> --json
```

scaffold 只提供可原样 validate/build 的 typed identity 示例，不是研究算法。项目入口 ABI 固定为
`run(context, inputs, output_root)`：`context` 提供项目/运行/节点/attempt、带时区 clock、seed 和参数；
`inputs` 是 Worker 已复验的 typed 输入；源码只能在 `output_root` 下写文件，并返回与声明端口、
Artifact 类型、相对路径、内容 hash、schema hash 和字节数一致的 commit。Worker 会拒绝未声明文件、
端口或类型不闭合及内容漂移。

项目源码的贴身测试至少覆盖：手算 oracle、PIT、标签隔离、时间边界、空/重复/缺失数据、确定性和负控制。计划发布后，run 和恢复只消费计划内闭包。

## 校验运行原则

运行任何检查前先回答：它会发现什么具体失败；失败后下一步会改什么。答不上来就不运行。相同事实只校验一次并让下游直接消费，不用评分表替代明确判断。

## RD-Agent 可选集成

`integrations/rdagent` 使用 RD-Agent LoopBase 调度研究，公式编码使用 CoSTEER。普通 RP 命令不加载 RD-Agent 或模型客户端。模型身份、代理和凭据从显式仓库外 `.env` 读取。

公式复现固定已经确认的定义，最多三次编码尝试。候选代码经项目 bundle 执行，同一主链产生 Result 与 VerificationResult；修复反馈只包含技术与公式正确性，不以收益奖励改写原公式。

`campaign-run`、`campaign-inspect` 与 `campaign-resume` 管理多轮开发研究。预测校准模式消费已经验证的开发预测；研究包模式为允许的参数变体展开普通 ResearchPackage，每轮实际运行、独立验证和报告。预算、父候选、停止条件和过程记录共用同一实现。

研究包循环拒绝最终 holdout 节点，输入和评价限冻结开发范围。反馈只投影指定表的指定指标，验证失败不能进入有效选优；最终研究须在开发选择完成后单独声明和执行。研究过程的最佳开发值不能继承任何旧 holdout 成绩，也不能视为未经样本外验证的策略收益。

候选标识写入 Workspace allocation.label，中断后找回同一 execution。已封存 Result 只续验或续报，已完成轮次不重新运行；RD 快照回退不撤销 RP 执行事实。用法与公开教学例见[开发研究循环](../integrations/rdagent/docs/research-campaign.md)。
