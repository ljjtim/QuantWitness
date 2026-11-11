# 因子研究

因子研究从已确认的基线开始。
后续提案只能选择冻结空间中的公式。
每轮通过Workspace执行正式研究包。
循环只反馈成功Result中的开发指标。

## 准备材料

准备完整ResearchPackage、Catalog和输入归档。
项目算子bundle实现特征、标签和汇总。
日频Qlib包提供`feature`、`label`、`summary`和`model_fit`节点。
前3个节点的`design`保持一致。
模型节点保存同一研究身份。

请求使用`rd-factor-research-v2`。
请求包含以下字段。

| 字段 | 内容 |
| --- | --- |
| `campaign_id`、`session_root` | 会话身份与新目录 |
| `package_template` | 只有1个固定基准的PackageCampaign请求 |
| `confirmed_spec` | 合成教学定义或已人工确认的公式材料 |
| `baseline` | `expression`、`hypothesis`、`reason` |
| `factor_space` | 完整冻结研究空间 |
| `budget` | 轮数、调用数、总输出额度和单次输出额度 |
| `proposer` | 固定响应或显式live模型身份 |
| `knowledge` | 可选的输入索引、输出索引和检索上限 |

## 声明研究空间

请求与正式`design.factor_space`必须相同。

```json
{
  "space_id": "example-signal-study",
  "fields": ["signal"],
  "max_window": 3,
  "expression_contract": "只使用前一完整会话及更早输入，公式由项目算子解释。",
  "feature_slot": "candidate_signal",
  "candidates": [
    {"expression": "$signal", "direction": "baseline"},
    {"expression": "Ref($signal, 3)", "direction": "lag"}
  ],
  "population_feature_columns": ["candidate_signal__value"]
}
```

`space_id`只标识空间，不触发框架分支。`feature_slot`必须已存在于`design.feature_expressions`。基线必须属于`candidates`。

框架按忽略空白后的文本匹配候选。框架返回项目声明的规范文本。候选表达式不能重复，方向不能为`stop`。

项目算子负责字段、窗口、缺失和时点规则。

提案只替换声明槽，其他特征保持原样。
框架同步更新研究设计与既有身份参数。
`population_feature_columns`使用正式samples表的完整列名。
联合研究按这些列检查共同资格。
缺失值不进入金融反馈。

## 运行合成示例

前置条件：已安装核心ml依赖、可选集成和固定RD-Agent。
`WORK`指向仓库外目录。
在公开源码根执行命令。

```bash
python integrations/rdagent/examples/factor_research/prepare.py --output "$WORK/factor"
python -m quantwitness_rdagent factor-run --request "$WORK/factor/request.json"
python -m quantwitness_rdagent factor-inspect --request "$WORK/factor/request.json"
python -m quantwitness_rdagent factor-resume --request "$WORK/factor/request.json"
```

示例枚举4类收盘价公式及有限窗口。
准备命令生成合成Parquet与完整声明。
固定响应验证研究流程，不调用付费模型。
真实公式复现使用`confirmed_formula`材料。
合成示例使用`synthetic`定义。
两者都必须与实际基线一致。

## 预算与恢复

预算字段为`rounds`、`model_calls`、`output_tokens`和`max_output_tokens_per_call`。
单次输出不超过8192 token。
提案、反思和方向选择共用预算。
使用live模式时，准备和运行都传入`--model-env-file`。
模型地址和身份必须与冻结请求相符。

`factor-resume`继续原请求和原目录。
改变空间、材料或输入时创建新会话。
已预约但结果未知的调用不会自动重试。
跨会话使用方式见[研究知识](coding-knowledge.md)。
