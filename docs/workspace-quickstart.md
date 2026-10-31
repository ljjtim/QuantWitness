# 入门教程：完成第一项研究

从股票横截面示例开始：四只虚构证券，按决策时已知的收益排序，再观察之后的收益差。研究包与算子已填写完整。目标是得到正式 Result 与报告，再改一个样本范围并比较两次结果。

你会完成这条流程：**准备合成行情 → 建立研究工作区 → 执行研究 → 打开报告**。不需要模型服务或真实行情；本例先教会你判断研究有没有正确完成，再接入 AI。

| 你会遇到的名称 | 在这个教程里是什么 |
| --- | --- |
| ResearchPackage，研究包 | 已填好的研究说明：用哪些数据、怎么算、输出什么 |
| Workspace，工作区 | 保存每次实验的目录 |
| bundle，扩展包 | 示例算法的封装文件 |
| Result | 正式结果表、运行摘要和来源引用 |

## 1. 准备环境

从 [QuantWitness 公开仓库](https://github.com/ljjtim/QuantWitness)取得完整公开源码，示例位于其中的 `examples/`；仅安装 wheel 不包含示例源码。在 QuantWitness 源码根目录执行本页 PowerShell 命令；单仓库用户先进入 `research_pipeline/`。从源码可用 `python -m pip install -e ".[dev]" "pyarrow==21.0.0"` 准备依赖；示例 bundle 明确锁定 PyArrow 21.0.0，`dev` 包含本页负例检查所需的 pytest。分发与安装范围见[安装说明](release.md)。

## 2. 准备合成输入

指定一个不存在的仓库外目录。示例默认放在源码所在磁盘的相邻目录；也可以将 `$work` 改成容量足够的数据盘路径。准备脚本会在此创建一次性合成 DuckDB 和 Catalog，不读取个人数据库；后续准入和运行只读访问这个合成数据库。需要遵守所在环境的数据库写入授权规则。

```powershell
$work = Join-Path (Get-Location).Path '../quantwitness-work/first-study'
if (Test-Path -LiteralPath $work) { throw '请选择不存在的研究目录' }
New-Item -ItemType Directory -Path $work | Out-Null
$env:PYTHONIOENCODING = 'utf-8'
$env:PYTHONUTF8 = '1'
$env:PYTHONDONTWRITEBYTECODE = '1'
$env:PYTHONPATH = (Resolve-Path -LiteralPath 'src').Path
$env:TEMP = Join-Path $work 'tmp'
$env:TMP = $env:TEMP
New-Item -ItemType Directory -Path $env:TEMP | Out-Null

$environmentText = python examples/prepare_synthetic_environment.py --output "$work/input"
if ($LASTEXITCODE -ne 0) { $environmentText | Write-Output; throw '合成环境准备失败' }
$environment = $environmentText | ConvertFrom-Json
$bundleText = python examples/build_bundles.py --output "$work/bundles"
if ($LASTEXITCODE -ne 0) { $bundleText | Write-Output; throw '项目 bundle 构建失败' }
$bundles = $bundleText | ConvertFrom-Json
$project = $bundles.projects | Where-Object { $_.name -eq 'equity_cross_section' }
$environmentText | Set-Content -LiteralPath "$work/environment.json" -Encoding utf8
$bundleText | Set-Content -LiteralPath "$work/bundles.json" -Encoding utf8
```

构建器统一生成四个示例的 bundle；本页只使用横截面项目。保存的 JSON 可在新的 PowerShell 会话中用 `Get-Content -Raw | ConvertFrom-Json` 重新加载。

## 3. 从现成研究包建立工作区

`--from-package` 校验并复制示例的四份 YAML；目标目录必须不存在，源码示例保持不变。不提供该参数时，`workspace init` 仍生成待填写的中性草稿。

```powershell
$workspace = Join-Path $work 'workspace'
python -m research_pipeline workspace init $workspace --from-package $project.package --json
if ($LASTEXITCODE -ne 0) { throw 'Workspace 初始化失败' }
```

## 4. 执行研究并打开报告

`workspace execute` 在同次调用内准备和准入研究包，然后分配并执行研究、发布 Result 和生成报告。普通 execute 不先完整运行独立 lint；需要主动诊断时可单独调用 package lint。时点与种子取自研究包，显式传入不同值会被拒绝。Workspace 与 RD-Agent 正式包执行共用应用层主链。

```powershell
$flowText = python -m research_pipeline workspace execute --workspace $workspace --catalog-lock $environment.catalog_lock --data-db $environment.database --extension-bundle $project.operator_bundle --label '股票横截面' --json
$flowExit = $LASTEXITCODE
$flowEnvelope = $flowText | ConvertFrom-Json
$flow = $flowEnvelope.data
$execution = $flow
$executionRoot = $flow.execution_root
if ($flowExit -ne 0 -or $flow.status -cne 'succeeded' -or $flow.execution_status -cne 'succeeded') { $flowText | Write-Output; throw '流程未通过；查看 failed_stage、stages 和下一步建议' }
$run = $flow
$store = $flow.result_store
$result = $flow.result_directory
Get-Content -LiteralPath $flow.report_path
```

### 怎么判断自己跑通了

先看 `status` 和 `execution_status` 是否为 `succeeded`，再打开 `report_path`。`result_directory` 指向正式结果，`result_store` 指向存储根；保留它们，后续报告和比较不必重新计算。结果中的 claim 与限制沿用研究声明。

到这里就完成了第一项研究。想接入自己的行情，转到[基本数据接入](catalog.md)；想尝试 AI，转到[PDF 因子复现](../integrations/rdagent/docs/formula-reproduction.md)。下面的变体比较和恢复是可选练习。

保存第一次结果，供下面的比较使用：

```powershell
$firstResult = $result
$firstStore = $store
$firstRunRoot = Join-Path $executionRoot 'run'
```

返回值中的 `stages` 是本次调用的阶段结果。运行成功要求 Result 已发布；报告失败会保留执行成功状态和结果位置。新 execution 的计划、工件、结果与报告位于独立目录。

本例 A 与 C 的未来收益差约为 `0.0472222222222223`（小数收益单位）。报告展示正式指标与声明限制，具体数值表保存在 Result 中；本例不包含交易费用或实盘成交模型。

## 5. 可选练习：改一个范围并比较

在工作区 `package/spec/research.yaml` 中，把唯一请求的 `universe.instruments` 从四只证券改成 `[SYN.A, SYN.B, SYN.D]`，其他字段保持不变。只改工作区中的研究包。

重复“执行研究并打开报告”中的 execute 代码块，不重复初始化 Workspace，也不执行保存第一次结果的三行。每次 execute 都建立新 execution，不覆盖第一次计划或结果。

```powershell
python -m research_pipeline compare --left-result $firstResult --left-result-store $firstStore --right-result $result --right-result-store $store --json
```

去掉 C 后，已知期首尾证券为 A 与 B，收益差约 `0.0348039215686275`。`compare` 对比正式指标定义、单位、样本范围和数值。样本量或窗口不同会返回不可比较原因，研究者仍可读取两份 Result 解释差异。本例不包含交易费用或成交模型，观察差异不代表可交易收益。

### 看懂一个前视失败例

源码示例的负例测试把结果日数据伪装成决策时已知，生产算法应拒绝；另一个负例将观察记录的可见时间移到决策之后，该证券应被排除。可以单独运行，不创建数据库：

```powershell
python -m pytest -q -p no:cacheprovider examples/equity_cross_section/test_operator.py --basetemp "$work/pit-tests"
```

预期三项测试通过，含义是“正常手算正确，两个有问题的输入得到预期处理”，不是允许未来数据进入研究。

## 遇到中断：恢复和复用

进程中断或节点可重试失败时，沿用原 execution。先查看运行状态：

```powershell
python -m research_pipeline inspect --run-root "$executionRoot/run" --json
```

依据 `recommended_action` 选择下面的一条命令。中断后继续使用 `resume`：

```powershell
$runText = python -m research_pipeline workspace resume --workspace $workspace --execution $execution.execution_id --json
if ($LASTEXITCODE -ne 0) { $runText | Write-Output; throw '恢复失败；按当前运行的诊断处理' }
$run = ($runText | ConvertFrom-Json).data
```

只有具体节点失败且允许重试时才使用 `retry-node`；将 `project_analysis` 换成诊断给出的失败节点：

```powershell
$runText = python -m research_pipeline workspace retry-node --workspace $workspace --execution $execution.execution_id --node project_analysis --json
if ($LASTEXITCODE -ne 0) { $runText | Write-Output; throw '节点重试失败；按当前运行的诊断处理' }
$run = ($runText | ConvertFrom-Json).data
```

成功后使用本次恢复返回的 Result 生成报告。不要用不带 execution-id 的 execute 代替恢复，它会分配新的 execution。空的、尚未正式运行的 execution 没有可恢复的 invocation。

```powershell
$store = Join-Path $executionRoot 'results'
python -m research_pipeline report --result $run.result_directory --result-store $store --output "$executionRoot/recovered-report.md" --format markdown --json
if ($LASTEXITCODE -ne 0) { throw '报告生成失败' }
```

也可用 `workspace execute --execution-id <原编号>` 接续原执行及报告。已有报告不覆盖；需要新报告时指定不存在的路径。

若重新打开 PowerShell，先恢复原 `$work`、`$workspace` 及保存的项目 bundle 引用，再选择原 execution，不能重新 allocate 代替恢复：

```powershell
$workspace = Join-Path $work 'workspace'
$bundles = Get-Content -LiteralPath "$work/bundles.json" -Raw | ConvertFrom-Json
$project = $bundles.projects | Where-Object { $_.name -eq 'equity_cross_section' }
$indexText = python -m research_pipeline workspace inspect --workspace $workspace --json
if ($LASTEXITCODE -ne 0) { $indexText | Write-Output; throw '无法读取 Workspace 索引' }
$workspaceIndex = ($indexText | ConvertFrom-Json).data
$workspaceIndex.executions | Format-Table execution_id, status, execution_path
$executionId = Read-Host '输入上一步列出的原 execution_id'
$executionText = python -m research_pipeline workspace inspect --workspace $workspace --execution $executionId --json
if ($LASTEXITCODE -ne 0) { $executionText | Write-Output; throw '无法读取原 execution' }
$execution = ($executionText | ConvertFrom-Json).data
$executionEntry = $workspaceIndex.executions | Where-Object { $_.execution_id -eq $execution.execution_id }
$executionRoot = Join-Path $workspace $executionEntry.execution_path
```

`workspace inspect` 用于选择 execution；节点失败和恢复建议由 `inspect --run-root` 提供。

若创建新 execution，希望复用完成态 run，在对应的 `workspace execute` 或 `workspace run` 命令后追加 `--reuse-run-root $firstRunRoot`；可重复给出多个来源，按顺序尝试。身份不匹配时机会式复用正常回退执行。样本范围变化会改变相关输入，因此本页变体不要求数据节点必须复用。

对于不允许回退重算的节点，追加 `--require-reused-node <节点ID>`，可重复指定；缺少完成态来源或任何指定节点无法复用时，在计算开始前拒绝。对于终态失败 run，只追加 `--reuse-failed-run-root <失败run目录>`，由原运行服务验证已成功 checkpoint。失败态与完成态复用不能混用。金融口径、数据可见性、clock/seed 和节点身份要求见 [Runtime 与恢复](runtime.md)。

## 文件与后续研究

所有生成的数据库、bundle、工件、临时目录与报告位于 `$work`，不要提交到源码仓库。源码中的示例和本机研究数据库保持原样。`workspace execute` 编排上述完整流程；`workspace run`、`resume` 和 `retry-node` 仍只委托原运行或恢复服务，成功后可单独 report。

完整闭环熟悉后再进入[首次使用指南](getting-started.md)创建自己的 ResearchPackage。合成数据只用于理解流程；真实研究必须冻结自己的数据来源、可见时点、费用与样本外范围。
