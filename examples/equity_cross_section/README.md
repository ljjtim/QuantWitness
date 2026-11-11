# 合成股票横截面

比较同一时点的股票信号及未来收益。

## 开始前

- 输入：完整源码、合成数据和构建后的研究包。
- 输出：横截面观察、正式指标和报告。
- 命令从包含 `pyproject.toml` 的公开源码根执行。
- 尖括号内容是待替换参数，输出目录必须尚不存在。

## 使用顺序

1. 按示例总览准备合成环境。
2. 构建本项目 bundle 与完整包。
3. 准入构建回执返回的 package。
4. 执行并读取正式 Result。
5. 用手算输入检查排序及未来记录隔离。

本项目用四只虚构证券的连续日频记录。演示先在同一决策时点依据已知收益排序，再用之后的收益评价的截面研究。证券代码、价格和时间全部由仓库内确定性夹具生成。

`extension/source/operator.py` 是项目自己的排序算法。`synthetic.py` 是项目输入的唯一来源。`test_operator.py` 用手算值与未来修订负例检查信号没有使用决策后的记录。

使用 [示例构建入口](../README.md) 将 Operator bundle 和完整 ResearchPackage 写入指定输出目录。构建返回的 `package` 是实际执行包，`spec/research.yaml` 中的 `metric_definitions` 绑定生产算法和指标口径。项目逻辑保留在本目录，不注册公共 Operator。

正式流程为 `package admit → run → Result → report`。Result 保存指标、观测与输入来源。报告通过 `report --result PATH --result-store ROOT` 读取。合成 DuckDB 仅在指定临时目录生成，准入与运行只读访问。

## 已知限制

未验收真实市场数据或实际交易。合成指标不代表投资结论。

独立 `package lint` 用于主动诊断。普通正式流程从完整声明直接准入。
