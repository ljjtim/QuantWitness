# Qlib量价因子与选定Alpha基线

公开示例在已准入日线工件上提供三套固定因子声明。生产数值计算调用 Qlib0.9.7 原生算子，独立复核按因子名手算。声明、字段、历史跨度和窗口政策随研究封存，结果进入既有模型六节点和正式 Result。

| 集合 | 项数 | 依赖字段 | 默认窗口政策 | 前置历史 |
| --- | ---: | --- | --- | ---: |
| `volume_price_v1` | 8 | close、volume | full | 最多5会话 |
| `alpha158_selected_v1` | 22 | open、high、low、close、vwap、volume | qlib | 最多5会话 |
| `alpha360_selected_v1` | 18 | open、high、low、close、vwap、volume | qlib | 最多5会话 |

## 调用与封存

模块位于 `examples/qlib_portfolio/extension/`，因子计算只接收 DataFrame 和显式交易日历。输入索引为 datetime/instrument，日线字段含义、价格口径、成交量单位和证券池由原 Catalog/PIT 准入固定。

```python
from factor_baselines import factor_suite, evaluate_factor_suite

suite = factor_suite("alpha158_selected_v1")
features = evaluate_factor_suite(
    admitted_frame, suite, sessions=approved_sessions,
    output_start="2024-01-10", output_end="2024-04-30",
)
```

`factor_suite` 返回 `qlib-factor-suite-v1` 映射，包含集合名、基线、Qlib版本、窗口政策、全部依赖字段、最大前置历史，以及每项特征的 formula、fields、lookback、operators、window_sessions。调用方把声明正文写入研究配置；DataFrame.attrs 只方便本地诊断，不能替代正式封存。数值计算核对实际安装版本为0.9.7。

每项 `lookback` 是观察日之前需要加载的会话数量，`window_sessions=lookback+1` 是包含观察日的输入跨度。正式 Feature 行使用原因子名与该 window_sessions，模型列为 `feature_id__w{window_sessions}`。Alpha360 的 CLOSE0 只需当前收盘，CLOSE5 需要当前及五会话前的收盘；位移依据声明日历，缺失会话保留缺失。

## 量价公式

| 因子 | 公式及含义 |
| --- | --- |
| volume_ratio5 | 当前成交量 / 五会话平均成交量，分母加1e-12 |
| volume_change1 | 当前成交量 / 前一会话成交量 - 1，分母加1e-12 |
| volume_dispersion5 | 五会话成交量样本标准差 / 平均成交量，分母加1e-12 |
| price_volume_correlation5 | 五会话收盘价与 log(volume+1) 的相关性 |
| volume_weighted_return5 | 五会话价格收益率的成交量加权均值，权重和加1e-12 |
| up_volume_share5 | 五会话上涨价格变化乘成交量的和 / 全部绝对价格变化乘成交量的和，分母加1e-12 |
| down_volume_share5 | 五会话下跌价格变化乘成交量的和 / 全部绝对价格变化乘成交量的和，分母加1e-12 |
| volume_change_balance5 | 五会话成交量变化之和 / 绝对成交量变化之和，分母加1e-12 |

滚动量价特征包括观察日，价格收益和量的变化还需前一会话，因此依赖变化的五会话因子需要六根输入日线。成交量允许为零，不能为负；价格必须为正。输入缺失保留缺失，无穷值拒绝。分母的1e-12保留公式中明确声明的零量政策，不能替代缺失处理。

## 选定Alpha集合

Alpha158 子集直接读取 `Alpha158DL.get_feature_config()` 中这些原生定义：

- K线9项：KMID、KLEN、KMID2、KUP、KUP2、KLOW、KLOW2、KSFT、KSFT2。
- 当日相对价格4项：OPEN0、HIGH0、LOW0、VWAP0。
- 五会话9项：ROC5、MA5、STD5、MAX5、MIN5、RSV5、CORR5、VMA5、VSTD5。

Alpha360 子集直接读取 `Alpha360DL.get_feature_config()` 中 CLOSE、OPEN、HIGH、LOW、VWAP、VOLUME 的0、1、5会话位移，共18项。价格字段均除以观察日收盘价；成交量字段除以观察日成交量加1e-12。Alpha158 的 ROC5 是五会话前收盘 / 当前收盘，不能把它解释为常见的当前 / 历史 - 1。

`min_periods="qlib"` 保留原生部分窗口政策，滚动标准差使用样本标准差。相关性需要至少两组有效配对；按Qlib0.9.7口径，两列各自的窗口样本标准差不超过2e-5时输出缺失。`full` 为显式的完整窗口变体，每个滚动输入必须有五个有效会话，累计前置历史不足也保持缺失。原生部分窗口不会改变 Ref 位移或缺失会话。

## 时间与独立复核

当天完整日线只有在收盘数据已可见后才能成为特征；本例在下一会话09:30决定，观察时间取前一会话收盘，可见时间按既有 next_session_open 准入确定。模型样本继承前一会话15:00观察时间，样本会话仍是下一会话的决定日。模型切分按既有核心合同使用观察时间所属会话分组，purge仍依据真实决定时间、标签终点与可见时间；独立Verifier按同一冻结日历手算成员和角色。不能用当日收盘量价生成当日开盘决策。财报、成分、行业、复权和市场规则继续遵守现有历史可见性合同。

`factor_baseline_oracle.py` 只使用标准库。`validate_factor_suite` 核对固定集合、原生公式、依赖、窗口和版本；`independent_factor_values(suite, history)` 对按同证券、同日历排列的截至观察日行情逐项手算，返回浮点数或None。缺失会话应在 history 中保留对应的空字段行，不能压缩历史。正式Verifier从 Result 的原始日线、声明和Feature表核对数值、缺失状态、固定键全集和时间，生产求值器不参与独立数值重算。数值核对对price_volume_correlation5及同定义CORR5使用绝对1e-8容差；其余因子保持相对1e-10、绝对1e-12。原生滚动相关性在低波动价格与较大成交量窗口存在消减误差，合成正式数据的最大绝对差为2.04e-9；独立中心化手算保留八位小数精度，1e-6扰动仍拒绝。缺失状态、平坦阈值和时间事实使用原合同。

已有 RobustZScoreNorm/Fillna 和横截面处理器足够消费这些因子。训练统计仅在合格训练期拟合，预测复用封存状态；选定集合不附带标签、证券池或新的处理器拟合范围。

## 已知边界

独立手算覆盖这48项固定定义，Alpha158的22项与Alpha360的18项是选定子集。通用表达式模块可读取更多原生定义，完整Alpha158/360的独立金融验收不属于这份交付。

特征数值检查与未来扰动不能代替输入PIT准入、正式Result独立验证和最终holdout规则。量价与Alpha子集的正式研究闭环按公开示例的同一准入、运行、验证和恢复流程验收。没有真实市场收益结论。
