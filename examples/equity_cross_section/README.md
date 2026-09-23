# 合成股票横截面

本项目用四只虚构证券的连续日频记录，演示“先在同一决策时点按已知收益排序，再用之后的收益评价”的截面研究。证券代码、价格和时间全部由仓库内确定性夹具生成，不对应真实市场数据，也不提供交易结论。

`extension/source/operator.py` 是项目自己的排序算法，不能进入 `src/research_pipeline/`。`synthetic.py` 是项目输入的唯一来源；`test_operator.py` 用手算值与未来修订负例检查信号没有使用决策后的记录。

合成数据上的 ResearchPackage、临时 Catalog Lock、正式 `package admit → run → Result → verify → VerificationResult → report` 已通过验收，并在 verify 后重新 lint。合成 DuckDB 只在临时目录生成；准入和运行不修改它。此示例未验收真实市场数据或实际交易。
