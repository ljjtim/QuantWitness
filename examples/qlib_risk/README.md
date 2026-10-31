# Qlib风险组合研究

本例在公开合成ETF模型ResearchPackage上增加风险组合目标，并通过LEAN日频引擎执行。输入为封存Parquet，证券和行情均为合成；正式验收使用一次调仓，原持仓绑定引擎真实的初始全现金状态。

在QuantWitness公开源码根安装依赖并执行：

```powershell
python -m pip install -e ".[ml]" "scipy==1.15.3" "cvxpy==1.7.5" "clarabel==0.11.1"
python examples/qlib_risk/run.py --method inv --lean-home <完整LEAN安装目录> --output I:/quantwitness/risk-inv `
  --research-series-id synthetic-risk --research-series-root I:/quantwitness/holdout-series `
  --research-series-purpose "合成ETF风险组合最终确认"
```

`--method`支持inv、gmv、rp、mvo、enhanced和topk_dropout。`--estimator`支持empirical和shrink；`--lookback`默认20个完整收益会话。收缩模型固定alpha=0.1，目标为常方差矩阵。所有风险输入仅使用决策时已可见的价格变化。

风险方法、估计器和窗口共同进入冻结研究设计；模型拟合与最终holdout绑定同一份完整设计。每个风险配置分别封存ResearchPackage与Result。同一研究问题始终复用同一个持久研究系列；改变风险方法、输出目录或其他设计参数不会清零已经打开的最终holdout。最终确认前应完成开发期比较，同系列重叠日期只能打开一次。旧账本可通过重复的`--research-series-legacy-root`显式登记。

研究在第一个开发test预测日09:31生成目标，下一会话09:30执行，LEAN引擎处理整手、现金预算、停牌、涨跌停和费用。后续估计无需把目标权重当作实际持仓；适配接口接受显式的实际持仓及持有会话数。最终holdout只用于原模型研究评价，不流入组合目标。

目标证据保存于Result的risk_inputs、risk_optimization_results、risk_optimization_attempts与risk_constraint_residuals表。风险目标由生产优化器计算，LEAN引擎保存正式订单、意图、每日账户、策略轨迹和费用指标。生产指标定义保存在包内`spec/research.yaml`的`metric_definitions`，引用实际指标算法。完成以运行回执的`status=result_finalized`及正式Result为准。

可用`--stage prepare|lint|admit|run|resume|report`分阶段执行；恢复使用原输出目录与原冻结声明，不重新传入系列参数。系列目录应独立于运行输出长期保留，详见[研究系列合同](../../docs/holdout-series.md)。

报告通过`report --result PATH --result-store ROOT`读取正式结果。Workspace成功输出`status=succeeded`、`result_directory`、`result_id`、`result_store`和`report_path`。

## 已知限制

本例是单次实际初始持仓调仓与完整证据验收，不代表动态、多次实际持仓反馈。收益来自未复权合成价格；风险因子暴露为证券身份矩阵，增强指数基准为冻结的合成等权基准。真实研究需要决策时可见的基准成分、权重、风险暴露和实际持仓快照。

增强指数使用CLARABEL执行相同优化方程。求解器和版本进入结果；原约束失败、明确放宽、近似解、失败保留持仓与后处理越界分别保存。目标和实际成交可能因执行限制不同，二者分别保存在Result中。


风险目标、优化残差及输入证据继续保留。执行复用Qlib组合的LEAN策略、独立开盘来源和正式snapshot；自有来源使用`--input-config`显式提供完整LEAN配置，见[组合执行合同](../qlib_portfolio/README.md#lean组合执行)。旧日频现金、信用账户与外部资金流引擎不再执行。
