# 合成成交量集中度

本例包含教学PDF和2400条虚构分钟记录。
2只虚构证券各包含5个完整交易时段。
流程覆盖审阅、确认、受限编码和正式Result。
固定响应不调用付费模型或数据库。

## 公式与时点

日值为`C = sum(v_i²) / sum(v_i)²`。
3日值为最近3个完整会话日值的算术均值。
均匀成交的240分钟日值为`1/240`。
可见时间为当日最后一个已完成bar。
窗口保留完整日历位置，不跳过无效日。

| 输入情况 | 状态 |
| --- | --- |
| 缺少预期分钟 | missing_bars |
| 负数、非有限数或空成交量 | invalid_volume |
| 完整日总成交量为0 | zero_volume |
| 前2个会话缺少3日窗口 | warmup |
| 窗口内包含无效日 | invalid_window |

window_sessions支持1至5（含）。
公式复现请求固定3日。
两轮窗口比较见[参数研究](../package_campaign/README.md)。
候选只修改candidate/compute.py。
固定adapter负责日历、输入、状态和输出表。
开发测试使用独立手算值。

## 准备材料

Windows环境安装核心、pyarrow和materials依赖。
Linux环境安装固定上游RD-Agent。
依赖步骤见[安装指南](../../docs/installation.md)。

| 变量 | 内容 |
| --- | --- |
| WindowsPython | Windows环境的Python程序 |
| Repo | 含src和integrations的公开源码根 |
| Output | 尚不存在的Windows输出目录 |
| LinuxRepo | 同一源码根的Linux路径 |
| LinuxOutput | 同一共享输出目录的Linux路径 |
| Reviewer | 完成材料审阅的确认人 |

```powershell
& $WindowsPython "$Repo/integrations/rdagent/examples/volume_concentration/prepare.py" `
  --output $Output --windows-python $WindowsPython --windows-repo $Repo `
  --linux-repo $LinuxRepo --linux-output $LinuxOutput
& $WindowsPython -m quantwitness_rdagent spec-review --draft "$Output/draft.json" `
  --materials "$Output/materials.json" --output "$Output/review.md"
```

准备命令生成合成Parquet和最小Catalog。
Catalog绑定当前分钟能力清单。
输出另含研究包、算子bundle和归档PDF。
draft.json是待审草稿，不代表人工确认。
来源URL使用教学占位地址，实际材料为归档PDF。

## 确认与绑定

1. 阅读PDF、中文公式与review.md。
2. 将decisions-template.json另存为decisions.json。
3. 核对teaching_definition并填写review_notes。
4. 执行确认和请求绑定命令。

```powershell
& $WindowsPython -m quantwitness_rdagent spec-confirm --draft "$Output/draft.json" `
  --materials "$Output/materials.json" --decisions "$Output/decisions.json" `
  --confirmed-by $Reviewer --approve --output "$Output/confirmation.json"
& $WindowsPython -m quantwitness_rdagent request-build --template "$Output/request-template.json" `
  --draft "$Output/draft.json" --materials "$Output/materials.json" `
  --decisions "$Output/decisions.json" --confirmation "$Output/confirmation.json" `
  --output "$Output/request.json"
```

教学定义没有未决歧义，resolutions为空。
自动化fixture署名使用synthetic-test-fixture。
fixture署名不代表真实材料的人工审阅。

## 执行与恢复

RD_REQUEST指向共享目录request.json的Linux路径。
在固定RD-Agent环境执行命令。

```bash
"$RD_AGENT_PYTHON" -m quantwitness_rdagent run --request "$RD_REQUEST"
"$RD_AGENT_PYTHON" -m quantwitness_rdagent inspect --request "$RD_REQUEST"
"$RD_AGENT_PYTHON" -m quantwitness_rdagent resume --request "$RD_REQUEST"
```

candidate-0000是预期失败案例。
失败保留算子诊断，不生成Result引用。
candidate-0001完成正式执行并发布Result。
反馈不包含收益筛选指标。
每个候选通过公开构建、准入、执行和报告。

check_recovery.py核对公共恢复和编码前快照恢复。
RECOVERY_RECEIPT指向尚不存在的收据文件。

```bash
"$RD_AGENT_PYTHON" "$QUANTWITNESS_REPO/integrations/rdagent/examples/volume_concentration/check_recovery.py" --request "$RD_REQUEST" --output "$RECOVERY_RECEIPT"
```

## 输出与限制

示例日期为2025年1月6日至10日。
示例不包含收益标签、选模或交易仿真。
运行目录保存候选、日志、临时工作区和Result引用。
清理前保留正式Result及实际输入依赖。
新公式使用新的研究包、指标声明和冻结请求。
