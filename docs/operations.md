# 运行维护与问题排查

诊断运行故障，并选择合适的恢复动作。

## 开始前

- 输入：原运行目录、计划、工件和诊断记录。
- 输出：只读诊断、恢复建议及回收预览。
- 命令中的尖括号内容是待替换参数。

## 使用顺序

1. 先运行 `inspect`。
2. 按故障需要运行 `doctor`。
3. 使用诊断建议的唯一恢复动作。
4. 保存失败材料及正式结果。
5. 清理前先查看 `gc` 预览。

这页面向已经开始运行研究的人：环境哪里有问题。研究执行到哪一步，怎样继续，以及哪些输出可以清理。保留正式结果及其输入与恢复材料，再处理缓存和临时目录。


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

## 处理规则

安装依赖、离线构建和最低 Python 验收见 [安装与发布构建](release.md)。

## inspect、doctor 与恢复

先用 `inspect` 看单个 run 的节点、attempt、checkpoint 和下一动作：

```powershell
python -m research_pipeline inspect --run-root <run目录> --json
python -m research_pipeline doctor --run-root <run目录> --json
```

`doctor` 只读检查当前 Runtime 和扩展注册，不修数据库、不改 Result。

运行项目扩展前需信任其源码和依赖。worker允许正常临时文件与替换操作，不限制任意本地副作用。仅正式提交路径和内容由框架控制。

普通目录、受控junction和hardlink均按真实路径核验，越出正式根仍拒绝。独立worker、超时和进程树清理用于故障隔离，不代表数据库写入授权或恶意代码沙箱。

`inspect` 会返回最后错误、尝试次数和余额、正式 checkpoint 复验原因、Result finalize 状态以及唯一建议命令。该过程只读。恢复规则：同身份中断用 resume。

错误码在 retry policy 内且仍有余额才用 retry-node。可重试故障耗尽或取消后的终态重算用 rerun-from。非重试错误和任何身份变化都要修正 package/算子合同、重新 admit 并新建 run。

缺少 finalize 投影时显示 `unknown`，不会把 Runtime succeeded 当成完整研究成功。

run 内的工件验证复用不落盘。即使 checkpoint 可复用，新的 `resume` 进程也会重新验证它实际
打开的 ExternalArtifact、DatasetArtifact 和分钟分区。`rerun-from` child 同样建立自己的验证
会话。

若新会话发现内容、manifest、marker、schema、分区或路径身份变化，停止恢复。调查工件来源。禁止复制旧会话状态或写持久 hash 缓存绕过检查。

data 节点在第三个或更后 request 失败时，不要删除 run 内的 `request-recovery`。按既有错误码
执行 `retry-node` 或在中断后执行 `resume`，Runtime 会复验已完成 request 的
DatasetArtifactRef 并只重做失效项。partial index 不是正式输出。

禁止手工复制到其他 run 或交给下游。禁止编辑 plan hash 或引用来强制复用。无法解析的索引会被当作没有恢复记录。

## 垃圾回收

```powershell
python -m research_pipeline gc --root <工件根> --ttl-seconds 86400 --json
python -m research_pipeline gc --root <工件根> --ttl-seconds 86400 --apply --json
```

第一条只生成计划，列出候选文件数、逻辑字节和保护原因。`--apply` 将候选移入 `quarantine/`，不删除隔离载荷，不代表磁盘空间已经释放。

当前命令扫描工件根下的 `staging/`、`cache/` 及 external 暂存目录。external 暂存必须具备生产者停止、Runtime owner 停止和 attempt 终态证据。未知归属和活动对象保留。

成功节点的已交接载荷按 [产物生命周期](artifact-lifecycle.md) 逐文件释放，失败恢复材料和正式输出保留。GC 不扫描、不移动 ResultStore、完成态 run-root 或历史项目轮次，也不会自动建立这些目录之间的引用关系。真实研究归档必须另行保留 Result 本体及其引用的持久输入。

验收摘要不能替代它们。完成态 run-root 只有在 `report`、`export-result` 已证明不再依赖它，并经过独立清理规划和授权后才可处理。

仍被故障排查、人工记录或其他工具直接引用的 run-root 继续保留。

## Workspace 与 Dashboard

Workspace 为每次新研究分配 execution。显式 execution-id 接续原运行。普通 execute 完成准备、准入、run 与 report，独立 lint 用于主动诊断。

Dashboard 直接读取 Result。工作区清单为 research-dashboard-workspace-v4，条目包含 entry_id、label、result、result_store。run_id/runtime_run_id 与 Result 身份必须一致。

缺失、损坏或错绑结果不能进入清单。没有可用结果时移除上一次导出的派生清单。报告与 Dashboard 不连接研究数据库，不重跑交易或提高声明结论级别。

## 数据库

research_pipeline 的数据库访问只读。采集、修复、建表、因子重算和发布不属于运维命令。这些操作必须在对应子系统获得单独授权。

## 故障处理

- 保留失败 run、事件和结构化错误码，先找根因。
- 不编辑 checkpoint、Result 或 plan 来“修通过”。
- 输出目录冲突时使用新目录，不覆盖已提交工件。
- 资源不足时显式调整额度或研究范围，不静默改 seed、样本或算法。已发布 Result 的报告失败只重试报告，不重新消费 holdout。
- Catalog/PIT 阻断时返回数据准入阶段，不能在 Runtime 绕过。

## 检查原则

只运行会改变后续动作的检查。代码改动跑贴身测试。跨层主链变更再跑全量测试、Ruff、compileall、能力 containment、clean wheel 和文档审计。
