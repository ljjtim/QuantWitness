# 合成 ETF 时间序列

两只虚构基金各自按已完成日线计算动量，不以其他基金的当日信息组成截面。样本、价格和下一交易日开盘时间由 `synthetic.py` 固定生成；`extension/source/operator.py` 仅放项目内的时间序列信号与下一日收益观察。

合成数据上的 ResearchPackage、临时 Catalog Lock、正式 `package admit → run → Result → verify → VerificationResult → report` 已通过验收，并在 verify 后重新 lint。合成 DuckDB 只在临时目录生成；准入和运行不修改它。交易费用、真实市场可交易性与实盘表现尚未验收。示例值不是投资建议。
