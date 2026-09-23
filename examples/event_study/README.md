# 合成公告事件研究

虚构公告同时记录事件发生、首次可见和修订可见时间。项目 extension 在决策时点选择当时可见的版本，再从事件后的行情观察窗口收益；同一标的重叠事件按项目声明的最小间隔处理。

输入由 `synthetic.py` 生成，`test_operator.py` 检查未来修订不能改变已经冻结的事件选择。合成数据上的 ResearchPackage、临时 Catalog Lock、正式 `package admit → run → Result → verify → VerificationResult → report` 已通过验收，并在 verify 后重新 lint；准入和运行不修改临时合成 DuckDB。未验收真实公告源与真实交易，这里不作因果推断。
