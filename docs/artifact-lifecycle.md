# 产物生命周期与回收

区分正式交付物、恢复材料和临时副本。

## 开始前

- 输入：运行目录、工件目录和实际保护状态。
- 输出：释放记录，或只读回收预览与隔离结果。
- 命令中的尖括号内容是待替换参数。

## 使用顺序

1. 根据目录表识别产物用途。
2. 先运行不含 `--apply` 的 `gc`。
3. 检查候选和保护原因。
4. 长期保存正式结果及实际输入依赖。

Runtime 自动释放本次成功节点、成功月分区或成功因果键批已经交接的副本。正式 ExternalArtifact、checkpoint、Result 和持久输入不会因为某次运行成功而删除。节点回收记录位于 `work/<attempt>/artifact-lifecycle.json`。

月分区记录位于 `work/<attempt>/partitions/<月份>/attempt-<次数>/artifact-lifecycle.json`。其中 `released_files`、`released_bytes` 是实际删除成功的文件数和逻辑字节。保留和隔离另行记录。

## 目录用途、提交与恢复

| 对象 | 用途与正式提交位置 | 恢复与保护 | 处理 |
| --- | --- | --- | --- |
| `request-recovery/<node>/data` | 普通列式请求的物化副本。完整数据节点把数据复制到 committed external | 失败或部分请求需要原副本。全部端口校验、资源通过、checkpoint 发布、attempt 和 node 成功后，确认正式 external 中每个替代文件存在 | 仅本次登记的成功数据副本自动释放。保留目录及 `partial-index.json` 诊断记录 |
| `work/<attempt>/outputs` 中已提交载荷 | Worker 结果原件。提交到 external 后由 typed 端口交接 | 必须确认 Worker 进程树退出。普通节点要求全部输出完成提交，分区节点要求本分区状态、资源和checkpoint完成交接 | 自动释放逐项登记的文件。不删除整个 `work`，回执、代码、诊断保留 |
| 分区 Worker 状态载荷及状态输入副本 | 分区之间传递状态。正式 external 和分区 checkpoint 保存恢复依赖 | 每月资源验收及checkpoint成功、Worker退出后，下一月从正式工件读取state。失败分区材料保留 | 仅释放本次登记且已交接的输入和输出副本，无须等待完整节点结束 |
| 成功因果键批工作副本 | 本次已登记的 `causal-*` 工作目录 | 正式提交、核心时间事实和完整节点资源验收均已完成。活动 Worker 会话允许在任务交接后继续使用 | 追加 `causal-handoffs.jsonl` 后释放。carry 状态保留到下一批成功消费 |
| 失败因果键批、未完成交接或缺少替代文件的输出 | 恢复诊断和状态依赖 | 保护标记、失败材料和仍被后批使用的状态继续有效 | 保留 |
| `staging/<attempt>` | checkpoint 发布前准备 | 当前 Runtime owner、attempt 终态、COMMITTED 与 pin/lease/active 都参与保护 | 达到 TTL 且无保护时可隔离。缺少 owner 停止证据的旧 run 保留 |
| `external-artifacts/.staging/<token>` | external 检查和发布前暂存 | 新生产者在 `.staging-owners/<token>/` 自动记录归属及 `.active`。写入作用域结束、Runtime owner 停止、attempt 终结才允许考虑回收 | 满足证据、保护和诊断 TTL 后可隔离。缺少记录的历史暂存保留 |
| 根下 `.staging`、`.staging-*` | 其他生产者的暂存布局 | 没有接入当前 external owner 协议 | 只展示字节和保护原因，保留 |
| `cache/<entry>` | 原有缓存回收范围 | COMMITTED、pin/lease/active，以及存在的运行 owner 证据 | 沿用预览后隔离。不自动删除缓存 |
| `input-artifacts`、归档输入、原始行情 | 冻结计划的研究来源，可被多个运行共享 | 不因本次交接解除其他计划的引用 | 保留 |
| committed external、checkpoint、事件、invocation | 下游输入、inspect、resume、rerun 和跨运行复用 | 失败 run 内的成功节点仍可能有效 | 保留 |
| Result、配置、实现及输入引用 | 正式交付与直接结果消费 | 迁移时携带持久输入依赖 | 保留 |
| holdout 账本及研究系列锚点 | 限制留出集重复消费 | 失败消费和已打开区间不能重置 | 保留 |

## 因果键批释放

因果键批先完成正式外部提交、核心时间事实检查和完整节点资源验收。
系统记录 `causal-handoffs.jsonl` 后，才释放工作副本。
任务完成与 Worker 退出分别记录。
活动会话允许在任务完成后释放已交接副本。

`carry` 状态需要留到下一批成功消费并交接。
后续批次失败时，上一有效状态和当前失败目录一起保留。
节点结束时最多保留最后 1 批状态目录。

保护或清理失败时，系统记录保留原因。
清理失败不改变已完成批次的金融状态。
正式工件、失败材料和 holdout 账本继续保留。

## 成功释放顺序

普通项目月分区先提交正式输出和state，同步采样并验收节点累计资源峰值，再提交分区checkpoint。下一分区state引用正式external工件，随后仅释放本次登记的状态输入副本和输出载荷。采样峰值不随释放归零。

副本交接期间的实际空间仍计入预算。分区失败不释放本次材料，已交接月份的恢复身份和checkpoint保持不变。

LEAN Result 保留 orders、intents、daily、trace、fills、positions、account_events、lifecycle 和 metrics，必要账户与策略支持文件随结果保存。行情、规则、公司行动和信号来源仍由持久工件及计划引用。不能因为执行结果表数量更少，就把这些来源当作已被Result完整复制而删除。归档同时保存计划、输入引用及其实际依赖。

先完成各输出及状态的正式提交和复验，再完成资源验收与节点 checkpoint 发布。等待 Worker 已退出，写入 attempt 与 node 的成功事件，最后确认旧载荷的全部文件均有正式替代位置。若有必要条件不满足，系统保留原件。

源路径必须属于当前节点的请求恢复目录、当前 attempt 的 Worker outputs，或当前月分区实际复制并登记的 `inputs/runtime_state/content.bin`。正式替代路径必须位于该 external 对象内并属于它的文件清单。清理不重新计算内容摘要，继续使用既有 ExternalArtifact 验证。

源与替代文件尺寸无法对应时保留。`.pin`、`.lease`、`.active` 在源目录、子目录或 run 内祖先目录均可阻断释放。

只逐文件删除成功交接的载荷，按每次成功删除累计逻辑字节。删除失败会记录到回收报告，已提交研究状态保持成功。没有成功交接登记的历史目录不会由该机制补扫。中断后已有合法 checkpoint 继续走正式恢复，不为补清理而重新执行节点。

## 只读预览与隔离

```powershell
python -m research_pipeline gc --root <运行目录> --ttl-seconds 86400 --json
```

预览返回 `candidates`、`candidate_bytes`、`candidate_files` 与 `protected`。每个候选包含类别、相对路径、年龄、逻辑字节及文件数。保留对象提供 `reasons`。预览读取目录和现有状态，不打开数据库。

`--apply` 使用相同计划约束、根锁、路径边界和保护复查，把候选移入 `quarantine/`。计划生成后新增 pin/lease/active，或 Runtime owner 重新活动，均会阻断隔离。`--apply` 不删除隔离载荷，因此不代表空间已经释放。

原有 `purge_artifact_quarantine` 库函数默认预览，只有显式 `apply=True` 才删除过期隔离目录。公共 CLI 没有新增 purge 入口。

## 已知范围

历史目录清理需要单独授权。旧 external 暂存缺少归属和停止证据时，即使超过 TTL 也保留。TTL 只代表诊断保留期结束。

owner 登记失败会留下诊断和保守保护，不改变业务提交的成功或异常。小型 owner 记录不承担全局引用索引职责。

本机制不退休整个 run，也不回收共享输入、正式输出、holdout、项目候选源码或尚有消费者的状态。失败和部分完成的请求仍使用原恢复协议。因果键批交接只支持副本释放，恢复仍以节点为单位。目录及少量回执继续保留，逻辑释放字节不等于文件系统分配空间。


## 常用名称

| 名称 | 含义 |
| --- | --- |
| ResearchPackage | 数据、算法、样本、指标和资源的完整研究声明 |
| Catalog Lock | 已确认的数据字段、来源和可见性说明 |
| PIT（按时点可见） | 在过去某时刻只使用当时已知的信息 |
| QueryIR | 限定字段、范围、排序和预算的数据请求 |
| DAG（计算步骤图） | 节点之间的先后依赖 |
| Artifact（工件） | 带类型、内容身份和来源引用的产物 |
| checkpoint（检查点） | 通过正式验收的节点输出及恢复依据 |
| Result（正式结果包） | 正式表、运行摘要和配置、实现、输入引用 |
| holdout（最终留出集） | 开发选择之外的最终评价区间 |
| Supervisor | 监督执行、发布检查点和记录事件的主进程 |
| Worker | 执行节点计算的独立进程 |
| manifest（清单） | 记录文件、类型、范围和内容身份的文件 |
| schema（结构） | 表的字段名称、类型和约束 |
