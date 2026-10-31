# 合成股票横截面

本项目用四只虚构证券的连续日频记录，演示先在同一决策时点按已知收益排序，再用之后的收益评价的截面研究。证券代码、价格和时间全部由仓库内确定性夹具生成。

`extension/source/operator.py` 是项目自己的排序算法；`synthetic.py` 是项目输入的唯一来源。`test_operator.py` 用手算值与未来修订负例检查信号没有使用决策后的记录。

使用 [示例构建入口](../README.md) 将 Operator bundle 和完整 ResearchPackage 写入指定输出目录。构建返回的 `package` 是实际执行包，`spec/research.yaml` 中的 `metric_definitions` 绑定生产算法和指标口径。项目逻辑保留在本目录，不注册公共 Operator。

正式流程为 `package lint → package admit → run → Result → report`。Result 保存指标、观测与输入来源；报告通过 `report --result PATH --result-store ROOT` 读取。合成 DuckDB 仅在指定临时目录生成，准入与运行只读访问。

## 已知限制

未验收真实市场数据或实际交易；合成指标不代表投资结论。
