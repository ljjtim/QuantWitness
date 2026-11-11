# 合成期货期限结构

按已知结算价和到期信息选择合约。

## 开始前

- 输入：虚构合约、行情、合成环境和研究包。
- 输出：期限结构斜率、选择结果和报告。
- 命令从包含 `pyproject.toml` 的公开源码根执行。
- 尖括号内容是待替换参数，输出目录必须尚不存在。

## 使用顺序

1. 按示例总览准备合成环境。
2. 构建本项目 bundle 与完整包。
3. 准入构建回执返回的 package。
4. 执行并读取正式 Result。
5. 核对斜率和未来结算价隔离。

本项目使用虚构近月、次月合约，只按决策时点前已可见的结算价和到期信息选择合约。

`extension/source/operator.py` 计算期限结构斜率并选择当时仍可交易、流动性满足阈值的近端合约。`synthetic.py` 生成交易日、合约、价格和成交量。`test_operator.py` 检查斜率手算与未来结算价隔离。

使用 [示例构建入口](../README.md) 将 Operator bundle 和完整 ResearchPackage 写入指定输出目录。构建返回的 `package` 是实际执行包，`spec/research.yaml` 中的 `metric_definitions` 绑定生产算法和指标口径。项目逻辑保留在本目录，不注册公共 Operator。

正式流程为 `package admit → run → Result → report`。Result 保存指标、观测与输入来源。报告通过 `report --result PATH --result-store ROOT` 读取。合成 DuckDB 仅在指定临时目录生成，准入与运行只读访问。

## 已知限制

保证金、费用和真实市场可交易性尚未验收。

独立 `package lint` 用于主动诊断。普通正式流程从完整声明直接准入。
