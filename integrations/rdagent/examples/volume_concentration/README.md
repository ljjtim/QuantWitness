# 合成成交量集中度：第二公式案例

此例用两个证券代码、五个完整时段、2400条明确虚构的分钟记录，演示材料审阅、冻结请求、RD-Agent受限编码、RP正式执行与恢复。公式由本项目定义，不引用实证论文，不需要数据库、付费模型或原研究项目目录。

每天成交量为 `v_i`，日内集中度为 `C = sum(v_i²) / sum(v_i)²`。三日值为最近三个完整session的 `C` 算术均值。均匀成交的240个分钟对应 `1/240`。数据有两种不同的成交量形状，且每天变化。

- 缺少任一预期分钟：日指标记为 `missing_bars`，不填零。
- 负数、非有限数或空成交量：日指标记为 `invalid_volume`。正式分钟质量门禁也可能在公式执行前拒绝该输入。
- 完整日总成交量为零：记为 `zero_volume`。
- 前两个session的三日值：记为 `warmup`；窗口保留所有日历位置，不跳过无效日。
- 三日窗口内有任何无效日：记为 `invalid_window`。
- 指标可见时点为该日最后一个已完成bar。公式不访问以后日期。

默认窗口为三日；节点参数`window_sessions`可声明1至5日，输出`window_length`随结果封存。两轮参数研究见[正式研究包示例](../package_campaign/README.md)。公式复现请求固定三日。

候选仅修改 `candidate/compute.py`，adapter固定输入、日历、状态与输出表。第一份固定响应含候选计算错误，第二份响应实现上述公式；两份响应经真实RD Loop/CoSTEER调度，实时API调用数为0。公式数值与可见时点由开发期手算用例检查。

## 准备材料

在已有RP Windows环境执行，依赖版本以示例声明和[集成安装说明](../../README.md)为准。RP源码和集成均需能被该环境导入；`prepare.py`会依据 `--windows-repo` 装入本次仓库源码。示例使用 `pyarrow` 写出合成行情，材料定位需要 `pypdf`。示例自带仅含分钟数据的最小Catalog声明，在输出目录编译，不依赖个人默认目录锁。创建时读取所安装框架的当前分钟能力清单，并把清单身份和数据来源绑定写入本次 Catalog；后续执行与恢复继续使用这份固定记录。Linux使用集成README规定的固定RD源码环境。

所有路径由调用者明确提供；`--output`必须尚不存在。仓库参数优先指向公开QuantWitness根（含 `src/` 和 `integrations/`），也支持包含 `research_pipeline/` 的原仓库根。Windows与Linux输出路径必须指向同一共享目录。

```powershell
& $WindowsPython "$Repo/integrations/rdagent/examples/volume_concentration/prepare.py" `
  --output $Output --windows-python $WindowsPython --windows-repo $Repo `
  --linux-repo $LinuxRepo --linux-output $LinuxOutput
```

输出包含 `minute/`、当前批准的合成Catalog、归档输入清单、普通ResearchPackage、候选源码 bundle、`material/formula.pdf`、`source-archive/`、`materials.json`、`draft.json`、`decisions-template.json`和`request-template.json`。文件内的绝对路径仅指向本次显式输出与环境。

`prepare`不产生确认记录，也不启动研究。PDF是自编教学材料的英文公式文本，中文解释以本页为准；来源URL明确使用教学占位地址，实际来源为归档PDF。`draft.json`是待审草稿，不是模型提取的论文结论。

## 审阅并冻结请求

在已安装可选集成的Windows环境执行：

```powershell
& $WindowsPython -m quantwitness_rdagent spec-review --draft "$Output/draft.json" `
  --materials "$Output/materials.json" --output "$Output/review.md"
```

阅读材料与审阅稿，把 `decisions-template.json` 另存为 `decisions.json`，逐条核对后填写 `accepted_rule_ids`和 `review_notes`。本例唯一规则ID为 `teaching_definition`；`resolutions`为空，因为教学定义没有未决歧义。审阅完成后使用自己的署名确认：

```powershell
& $WindowsPython -m quantwitness_rdagent spec-confirm --draft "$Output/draft.json" `
  --materials "$Output/materials.json" --decisions "$Output/decisions.json" `
  --confirmed-by $Reviewer --approve --output "$Output/confirmation.json"
& $WindowsPython -m quantwitness_rdagent request-build --template "$Output/request-template.json" `
  --draft "$Output/draft.json" --materials "$Output/materials.json" `
  --decisions "$Output/decisions.json" --confirmation "$Output/confirmation.json" `
  --output "$Output/request.json"
```

自动化合成验收如使用fixture确认，署名必须明确包含 `synthetic-test-fixture`，并在验收记录中说明其教学测试性质；这不代表用户完成真实研究审阅。

## 执行、查看与恢复

Linux共享目录中执行正式集成入口，RD固定源码路径与环境安装见集成README。以下 `$RD_REQUEST` 是准备输出中 `request.json` 的Linux路径。

```bash
"$RD_AGENT_PYTHON" -m quantwitness_rdagent run --request "$RD_REQUEST"
"$RD_AGENT_PYTHON" -m quantwitness_rdagent inspect --request "$RD_REQUEST"
"$RD_AGENT_PYTHON" -m quantwitness_rdagent resume --request "$RD_REQUEST"
```

预期candidate-0000触发`NameError`，命令与执行状态均为`failed`，保留候选算子诊断且没有Result引用；candidate-0001完成正式执行，反馈没有收益筛选指标。每个候选走公开RP项目构建、准入、run和report。公式数值由开发期手算用例检查，成功Result直接交给报告与反馈。

完成后可运行同目录 `check_recovery.py --request "$RD_REQUEST" --output "$RECOVERY_RECEIPT"`，检查公共resume及编码前快照恢复不增加候选或execution，原run与Result文件集合和内容保持不变。正式事实以各候选Result为准。

## 限制与输出

示例固定为2025年1月6日至10日的五个同月时段。不含收益标签、选模、holdout或交易仿真，研究结论为数学教学观察。输入来源和完成bar条件由RP准入与生产入口检查。

运行产物全部保留在显式输出目录。`session/`中的候选、日志、临时工作区、运行和报告可用于查看，不应提交进源码仓库。其他公式使用新的研究包、指标声明和冻结请求，数学正确性通过项目算法用例验收。
