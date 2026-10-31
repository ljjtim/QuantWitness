# 合成公告事件研究

虚构公告同时记录事件发生、首次可见和修订可见时间。项目算法在决策时点选择当时可见的版本，再从事件后的行情观察窗口收益；同一标的重叠事件按声明的最小间隔处理。

`synthetic.py` 生成输入，`extension/source/operator.py` 计算事件观测与窗口收益均值。`test_operator.py` 检查手算收益、重叠控制，以及未来修订不能改变已冻结的事件选择。

使用 [示例构建入口](../README.md) 将 Operator bundle 和完整 ResearchPackage 写入指定输出目录。构建返回的 `package` 是实际执行包，`spec/research.yaml` 中的 `metric_definitions` 绑定生产算法和指标口径。项目逻辑保留在本目录，不注册公共 Operator。

正式流程为 `package lint → package admit → run → Result → report`。Result 保存指标、观测与输入来源；报告通过 `report --result PATH --result-store ROOT` 读取。合成 DuckDB 仅在指定临时目录生成，准入与运行只读访问。

## 已知限制

未验收真实公告源与真实交易；此例不作因果推断。
