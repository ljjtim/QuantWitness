# 待著而救日级因子联合研究

日级研究使用已经确认的分钟事件规则、20交易会话聚合和冻结行情。D日09:30的因子仅使用前一交易会话及更早完整输入。原月末观察保持原定义，日频特征由项目扩展和独立Verifier计算与核对。

## 请求与设计

因子请求继续使用 `rd-factor-research-v1`，增加显式 `factor_contract="dai-daily-v1"`。源 ResearchPackage 的 Feature、Label、Summary 设计共同声明：

```json
{
  "factor_contract": "dai-daily-v1",
  "feature_expressions": {"dai_following": "$dai"}
}
```

`baseline.expression` 必须为 `$dai`，`confirmed_spec` 沿用 `kind=confirmed_formula` 及 `path/draft/materials/decisions`；确认材料的 interface 明确包含 `$dai`。合成验收可使用已有 `kind=synthetic` 合同，定义、基线和合成来源须对应。模型请求沿用原合同，与因子请求共用源包、开发范围、目标及正式设计。

不声明新合同时仍使用四类收盘价合同。显式 `close-price-v1` 表示同一价格合同；两个合同不能混用字段、槽或金融知识。

## 表达式与方向

| 定义 | 规范表达式 | 方向 |
| --- | --- | --- |
| 已确认基线 | `$dai` | baseline，仅第一轮 |
| 历史滞后 | `Ref($dai, N)`，N为1至5 | lag |
| 历史均值 | `Mean($dai, N)`，N为2至5 | mean |

仅允许这十个定义。零值合法，任何所需值缺失则该特征缺失，不前填、不压缩交易日历，不增加其他字段、算子或组合。项目扩展在Fillna之前判定最多25会话完整事件历史的共同合格掩码，各轮使用同一掩码。

本项目联合请求显式声明 `research_schedule=["baseline","factor","model"]`，轮次数必须为3。第一轮复现基线，后两轮方向调用分别限定因子或模型分支；模型仍返回方向及实际采用的知识引用。不符合计划的响应被拒绝，不改写响应。未声明计划的既有会话自由选择分支。

日级因子方向为 lag、mean，模型方向为 generated_model；允许依据证据选择 stop。模型提案保存 `factor_record_id` 和 `factor_expression`，将已验证开发指标最好的因子写入 `dai_following` 槽。正式设计、研究身份、Result与独立验证覆盖绑定；模型阶段只改变受支持的生成式前馈网络结构。

## 正式证据

项目扩展提供分钟到日级值、独立Ref/Mean期望计算及源时间证据。正式Result须包含一致的研究设计、填充前特征、样本与标签时点、validation预测和指标，独立VerificationResult为pass。样本的日级特征列使用 `dai_following` 或其窗口列 `dai_following__...`。

共同资格门禁读取验证绑定的model_diagnostics，要求所有入选日级值完整，并逐条比较样本、标签起止与可见时点、fold及validation成员；候选值不同可以评价，成员或标签改变不能进入金融反馈。最终test/holdout事实不得进入此门禁、方向、提案、反思或知识索引。

独立Verifier核对共同掩码、原始$dai及Ref/Mean、缺失与零值、分钟完成时间和日级可见时间。标签按未复权D到D+1收盘价格变化定义，D+2开盘成熟；切分purge使用真实成熟时点。处理器仅在训练集拟合，填充不能扩大合格成员。

## 调用与恢复

使用已有 `joint-run/joint-resume/joint-inspect`。两分支共用一份预算和调用收据；本项目三轮上限为12次模型调用、每次8192输出token、总预约98304 token，修复和反思均计入。完成的调用和正式评价从原收据恢复，未知调用结果不自动重试。

日级请求与设计在会话中冻结，恢复不得换成价格合同、改槽或改输入。金融反馈只含通过独立验证的开发指标；技术失败保留记录但不携带金融结论。知识检索同时匹配因子合同与原有来源、资产、日历、价格口径、日期和指标条件。

## 范围

本入口不访问文件数据库，不采集或修复数据。实际计算、模型调用、证券与日期范围以本项目明确批准的请求为准；合成合同验收不能替代真实正式Result和独立验证。未复权价格变化不等于含分红总回报；开发负结果照常保存。
