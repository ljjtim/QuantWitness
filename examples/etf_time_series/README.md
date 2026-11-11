# 合成 ETF 时间序列

计算每只虚构 ETF 的时间序列动量。

## 开始前

- 输入：完整源码、合成环境和构建后的研究包。
- 输出：动量信号、收益观察及正式报告。
- 命令从包含 `pyproject.toml` 的公开源码根执行。
- 尖括号内容是待替换参数，输出目录必须尚不存在。

## 使用顺序

1. 按示例总览准备合成环境。
2. 构建本项目 bundle 与完整包。
3. 准入构建回执返回的 package。
4. 执行并读取正式 Result。
5. 核对手算动量和未来记录隔离。

两只虚构基金各自按已完成日线计算动量。样本、价格和下一交易日开盘时间由 `synthetic.py` 固定生成。

`extension/source/operator.py` 保存项目内时间序列信号与下一日收益观察。`test_operator.py` 检查手算动量、逐证券信号及未来记录隔离。

使用 [示例构建入口](../README.md) 将 Operator bundle 和完整 ResearchPackage 写入指定输出目录。构建返回的 `package` 是实际执行包，`spec/research.yaml` 中的 `metric_definitions` 绑定生产算法和指标口径。项目逻辑保留在本目录，不注册公共 Operator。

正式流程为 `package admit → run → Result → report`。Result 保存指标、观测与输入来源。报告通过 `report --result PATH --result-store ROOT` 读取。合成 DuckDB 仅在指定临时目录生成，准入与运行只读访问。

## 已知限制

交易费用、真实市场可交易性与实盘表现尚未验收。示例值不是投资建议。

独立 `package lint` 用于主动诊断。普通正式流程从完整声明直接准入。
