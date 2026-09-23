# 合成期货期限结构

本项目用虚构近月、次月合约验证“只使用决策时点前已可见的结算价和到期信息选择合约”。`extension/source/operator.py` 计算期限结构斜率并选择当时仍可交易、流动性满足阈值的近端合约；它不进入框架 registry。

交易日、合约、价格和成交量均为合成数据。合成数据上的 ResearchPackage、临时 Catalog Lock、正式 `package admit → run → Result → verify → VerificationResult → report` 已通过验收，并在 verify 后重新 lint；准入和运行不修改临时合成 DuckDB。保证金、费用和真实市场可交易性尚未验收。
