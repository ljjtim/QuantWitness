# Qlib 日频因果表达式

`examples/qlib_portfolio/extension/expressions.py` 在已准入的日频矩阵上使用 Qlib 原生算子计算特征。输入为 `datetime/instrument` 双层索引，价格、成交量等字段是普通列。模块不连接数据库、不生成 Qlib `.bin` 文件，也不修改全局 provider 或表达式缓存。

## 接口

表达式实现属于示例项目的正式算子 bundle，可连同源码复用于其他研究项目；不是框架公共算子。bundle 内从 `expressions` 导入，外部项目应将该模块纳入自己的扩展源码声明。

```python
from expressions import (
    validate_expression, evaluate_expressions, baseline_expressions,
)

spec = validate_expression("$close/Ref($close,5)-1", fields=["close"])
# spec.lookback == 5；前置历史为五个会话。
features = evaluate_expressions(
    frame,
    {"historical_return": "$close/Ref($close,5)-1"},
    fields=["close"],
    min_periods="full",
    sessions=approved_sessions,
    output_start="2024-01-10",
)
```

`validate_expression` 不导入 Qlib 或计算因子，返回包含 `expression`、依赖 `fields`、历史会话数 `lookback` 和 `operators` 的不可变声明，可用 `to_dict()` 写入研究配置。`evaluate_expressions` 返回同索引、同原始行序的 DataFrame；可选输出区间在计算完前置历史后裁切。`attrs["qlib_expressions"]` 包含实际窗口政策与每项表达式声明；正式项目应把这些声明纳入现有输入配置，不能依赖 Parquet 自动保存属性。

调用方提供充分前置历史与完整交易日历。显式 `sessions` 中某证券缺失的日期保持缺失，不把此前有行情的日期当成上一交易日；没有传入 `sessions` 时使用输入所有证券的日期并集。日期并集不能识别全市场同时缺失的一天，因此正式研究应显式传入批准的日历。索引必须是日频日期；上市前、停牌等可见性与填补政策由项目声明，模块不做前向或后向填补。

## 窗口与缺失值

- `min_periods="qlib"` 保留 Qlib 算子的原生缺失值和窗口政策。多数滚动算子使用 `min_periods=1`，但标准差、相关性等仍有各自的数学样本要求。
- `min_periods="full"` 为默认值。每个滚动算子都要求窗口内的算子输入完整；相关性要求两列均完整。窗口计数同样由 Qlib Count 计算，未满足时输出 NaN。整个表达式在累计依赖历史不足时保持 NaN。
- `Ref`、`Delta` 是离散位移，不把跳过缺失行作为位移。嵌套窗口累加历史需求，例如 `Mean($close/Ref($close,5)-1,20)` 需要24个前置会话。
- 最终无穷值会报错，研究必须明确零分母处理。NaN 保留给既有特征缺失政策或 Qlib 处理器；不会自动填为零。
- 不同输出区间在使用同样充分的输入历史时，交集输出应一致。不能仅加载输出区间再期待正确的滚动历史。

## 允许的表达式

使用 `$字段名`、有限数值、算术、单次比较，以及显式允许的 Qlib 算子。支持 Ref、Delta、Mean、Sum、Std、Var、Max、Min、Med、Mad、Rank、Count、Slope、Rsquare、Resi、IdxMax、IdxMin、Skew、Kurt、WMA、Quantile、Corr、Cov、Abs、Sign、Log、If 和对应的算术、比较算子。

窗口必须是有限正整数，默认单窗及累计历史不超过252。未知字段、未知算子、未来 Ref、Ref(0)、扩张窗及 EMA 都会拒绝。EMA 的递归历史并非有限整数窗口，不能用一个有限 `lookback` 冒充。模块仅生成 Feature，不生成未来标签；标签仍由项目根据实际经济问题、可成交价格和成熟时点声明。

因果表达式不能代替数据准入。当天收盘价构造的特征只能在收盘数据已可得后使用，不能用于当天开盘交易；财报、行业、历史成分、复权与市场规则仍走原有 Catalog/PIT 约束。横截面处理仅针对当时可见的声明证券池。

## 原生标准特征定义

```python
expressions = baseline_expressions("Alpha158", selected=["MA5", "ROC5"], fields=["close"])
features = evaluate_expressions(frame, expressions, fields=["close"],
                                min_periods="qlib", sessions=approved_sessions)
```

`baseline_expressions` 直接读取 Qlib `Alpha158DL/Alpha360DL.get_feature_config()`，不创建其数据加载器。可读取全部原生定义或显式选择特征名；不静默删除不支持的表达式，不附带默认标签、证券池或处理器。完整定义需要 open、high、low、close、vwap、volume。要保持原生基线窗口口径，应选择 `min_periods="qlib"`；使用 `full` 是显式的研究变体。独立验证能力以具体项目声明为准，通用表达式求值不等于全部 Alpha 特征已经有独立研究验收。

## 模型处理器

在原有处理器上增加 `ZScoreNorm` 与 `CSZScoreNorm`：

- `ZScoreNorm` 使用训练样本拟合均值和总体标准差，预测复用封存状态；拟合起止日由实际训练索引确定，不接受调用方覆盖。
- `CSZScoreNorm` 仅允许 `infer feature`，支持 `method="zscore"` 或 `"robust"`，在每个会话的可见截面内处理。
- `RobustZScoreNorm`、Fillna、DropnaLabel 与 learn 标签 CSRankNorm 保持原合同。

处理器没有改变最终 holdout、标签成熟、候选选择和独立验证规则。固定量价和选定Alpha子集的声明、独立手算与正式入口见[量价与选定Alpha基线](qlib-factor-baselines.md)。完整训练与预测说明见 [Qlib 模型研究](walk_forward_model.md)。
