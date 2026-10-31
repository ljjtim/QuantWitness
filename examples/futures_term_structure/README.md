# 合成期货期限结构

本项目使用虚构近月、次月合约，只按决策时点前已可见的结算价和到期信息选择合约。

`extension/source/operator.py` 计算期限结构斜率并选择当时仍可交易、流动性满足阈值的近端合约。`synthetic.py` 生成交易日、合约、价格和成交量；`test_operator.py` 检查斜率手算与未来结算价隔离。

使用 [示例构建入口](../README.md) 将 Operator bundle 和完整 ResearchPackage 写入指定输出目录。构建返回的 `package` 是实际执行包，`spec/research.yaml` 中的 `metric_definitions` 绑定生产算法和指标口径。项目逻辑保留在本目录，不注册公共 Operator。

正式流程为 `package lint → package admit → run → Result → report`。Result 保存指标、观测与输入来源；报告通过 `report --result PATH --result-store ROOT` 读取。合成 DuckDB 仅在指定临时目录生成，准入与运行只读访问。

## 已知限制

保证金、费用和真实市场可交易性尚未验收。
