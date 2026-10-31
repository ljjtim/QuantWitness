# 合成 ETF 时间序列

两只虚构基金各自按已完成日线计算动量。样本、价格和下一交易日开盘时间由 `synthetic.py` 固定生成。

`extension/source/operator.py` 保存项目内时间序列信号与下一日收益观察。`test_operator.py` 检查手算动量、逐证券信号及未来记录隔离。

使用 [示例构建入口](../README.md) 将 Operator bundle 和完整 ResearchPackage 写入指定输出目录。构建返回的 `package` 是实际执行包，`spec/research.yaml` 中的 `metric_definitions` 绑定生产算法和指标口径。项目逻辑保留在本目录，不注册公共 Operator。

正式流程为 `package lint → package admit → run → Result → report`。Result 保存指标、观测与输入来源；报告通过 `report --result PATH --result-store ROOT` 读取。合成 DuckDB 仅在指定临时目录生成，准入与运行只读访问。

## 已知限制

交易费用、真实市场可交易性与实盘表现尚未验收；示例值不是投资建议。
