# Workspace 模板复用与执行接续

Workspace 将完整 ResearchPackage 交给准备与准入、运行、Result 和报告服务。新研究建立新 execution；中断研究通过显式 execution_id 接续原执行。

## 从完整声明建立研究

```powershell
python -m research_pipeline workspace init <新工作区> --from-package <完整研究包> --json
python -m research_pipeline workspace init <新变体工作区> --from-package <完整研究包> --parameter target.contracts=2 --json
```

目标必须不存在。初始化复制四份声明及包内分钟输入，保留图、QueryIR、时点、种子、ResultSpec、来源、指标和金融口径。不提供模板时创建中性草稿。

parameter 仅替换模板已有的 `NODE.PARAM`，值使用 JSON。未知节点、未知参数、重复键和不兼容类型拒绝；JSON 对象整体替换。参数变化产生新包身份，需要重新准入。批量变体可使用 package expand-variants。

外部特征模板必须包含字段、键、可见时间和消费者；模型模板保留标签成熟时间、切分、训练与 holdout；组合模板保留持仓、交易规则、费用和估值。只提供名称或一个 CSV 不能补齐研究合同。

## 一次执行

```powershell
$workspace = 'I:/research/study'
$catalog = 'I:/research/inputs/catalog-lock'
$archive = 'I:/research/inputs/input-snapshot-manifest.json'
$operatorBundle = 'I:/research/bundles/operator'
$flowText = python -m research_pipeline workspace execute --workspace $workspace --catalog-lock $catalog --input-snapshot-manifest $archive --extension-bundle $operatorBundle --json
$flowExit = $LASTEXITCODE
$flow = ($flowText | ConvertFrom-Json).data
if ($flowExit -ne 0) { $flowText | Write-Output; throw '研究执行失败；按阶段诊断处理' }
$executionId = $flow.execution_id
Get-Content -LiteralPath $flow.report_path
```

数据库来源使用 data-db/source-db，与 input-snapshot-manifest 互斥。分钟根按研究输入显式提供。clock 和 seed 来自包；显式值必须一致。

普通 execute 在同次调用内合并准备与准入，一次解析、静态诊断和物理来源检查后将准备对象交给运行；不先完整执行独立 lint 再重复编译。持久计划仍为恢复保留。需要主动检查时单独运行 package lint。

成功返回 `status=succeeded`、`execution_status=succeeded`、`result_directory`、`result_id`、`result_store` 和 `report_path`。报告直接消费本次 Result。命令状态与执行状态分别记录：报告失败不改变 Result 已发布的事实。

## 接续原 execution

```powershell
python -m research_pipeline workspace inspect --workspace $workspace --json
python -m research_pipeline workspace execute --workspace $workspace --execution-id $executionId --catalog-lock $catalog --json
```

不指定 execution-id 的 execute 建立新执行。指定原编号时沿用原 plan、invocation、输入、预算和持久 holdout 锚点；不以新执行代替恢复。

| 原执行事实 | 接续动作 |
| --- | --- |
| plan 已发布、run 目录为空 | 加载原计划，启动原 execution 的 run |
| invocation 存在、尚无 Result | 依据 inspect 建议调用 resume 或 retry-node |
| 节点成功但 Result 未发布 | 原 resume 重试组装，不重复计算成功节点 |
| Result 已发布 | 核对身份、补齐 completion/index，再生成报告 |
| Result 与报告均存在 | 返回原结果，不重复研究或报告 |
| 输入、实现或记录身份冲突 | 拒绝接续，按诊断处理，不覆盖冲突记录 |
| 缺少正式恢复依据 | 拒绝猜测路径或重新训练 |

Result 发布和导航记录写入之间中断时，从正式 Result 恢复运行、计划和结果身份。已写 completion 而索引未更新时补齐索引；已有身份冲突立即拒绝。

continuation.json 只保存位置和调用参数。原 plan、invocation、Result 和 checkpoint 负责正式恢复依据。恢复不重置已打开的留出区间。

报告失败后可直接使用返回的 Result：

```powershell
python -m research_pipeline report --result $flow.result_directory --result-store $flow.result_store --output I:/research/report-2.md --json
```

目标报告必须不存在。workspace run、resume 和 retry-node 只执行或恢复运行服务，成功后可单独 report；execute 负责串起完整报告流程。

## 资源建议

运行前的 resource_forecast 展示真实节点资源声明、capacity、shortages、输入归档引用文件的字节和进程启动开销。数据库规模或峰值没有可靠依据时保持 unknown。

预算不足显示 configuration_insufficient；建议不自动提高额度、改变 workers、样本或 seed。并行总占用和实际峰值由运行时监管。资源建议不替代正式资源验收，见[资源预算](project_resource_budgets.md)。

## Dashboard 与保留

```powershell
python -m research_pipeline workspace dashboard --workspace $workspace --json
```

清单为 `research-dashboard-workspace-v4`，条目使用 entry_id、label、result 和 result_store，路径相对工作区生成根。只列与 execution 的 run/result 身份一致、可读取的正式 Result；没有可用结果时移除旧清单并返回诊断。

正式 Result、原计划和 holdout 账本按[工件生命周期](artifact-lifecycle.md)保留。输入引用的持久目录迁移时一并携带。
