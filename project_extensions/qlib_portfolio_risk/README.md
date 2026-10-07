# Qlib组合风险项目扩展

本扩展使用Qlib 0.9.7的协方差模型和普通组合优化方程，提供明确的求解状态与约束证据。目标输出接入已有日频现金引擎。

- `estimate_covariance`：按真实可见时点取固定历史窗口，调用Qlib经验协方差或固定系数、常方差目标的收缩协方差，输出小数收益平方单位。
- `optimize_portfolio`：逆波动inv、最小方差gmv、风险平价rp和均值方差mvo。SciPy求解结果包含success、status、message和目标值；逆波动使用解析权重。普通方法只做多、满仓，双边换手为sum(abs(w-w0))。
- `optimize_enhanced`：增强指数使用Qlib的因子风险、残差风险、基准偏离、强制持有/卖出和双边换手方程；每次CVXPY状态单独保存。默认CLARABEL，默认要求原约束成立。显式启用放宽或保留持仓后，结果分别标记relaxed_success、approximate或failed_retained。
- `topk_dropout`：采用固定bottom/top筛选，实际持仓和持有会话数决定卖出资格；保留证券维持实际权重，新买证券均分可用资金乘risk_degree，剩余为现金。持有期限阻止卖出时，持仓数可能暂时高于topk。

证券顺序显式绑定。零波动证券不进入逆波动/风险平价；全相同分数不执行波动缩放。均值方差输入明确为预期收益或标准化分数，scale_return进入证据。

`extension.py`的目标节点输出`research.portfolio-targets.v1`，包括targets、benchmarks及四类风险诊断表。正式ResearchPackage示例、依赖与结果范围见[风险组合示例](../../examples/qlib_risk/README.md)和[组合风险说明](../../docs/qlib-portfolio-risk.md)。

`oracle.py`使用独立数值表达式复算协方差、原始行情绑定、目标约束、求解目标值和Topk规则；不调用生产优化函数。正式金融门沿用现有独立现金、估值、费用及TCA oracle。
