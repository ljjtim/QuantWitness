# 正式研究包的参数研究

本例比较2日和3日成交量集中度窗口。
输入为2400条虚构分钟记录。
每轮通过Workspace准入、执行并发布Result。
RD-Agent只读取正式开发目标值。
固定提案器不调用模型或数据库。

## 目标

目标是2只虚构证券的滚动集中度均值。
评价日期为2025年1月8日至10日。
前2日输入用于预热。
两候选使用相同的6条目标样本。
最小化集中度仅用于演示候选选择。
较小数值不表示更好的投资效果。

参考值：3日窗口为0.004609909162671379。
2日窗口为0.004617528112477554。
按声明目标选择3日窗口。

## 运行

前置条件：已安装核心、materials依赖和固定RD-Agent。
在同一Linux环境从公开源码根执行命令。
RESEARCH_OUTPUT指向尚不存在的目录。

```bash
python integrations/rdagent/examples/package_campaign/prepare.py --output "$RESEARCH_OUTPUT"
python -m quantwitness_rdagent campaign-run --request "$RESEARCH_OUTPUT/request.json"
python -m quantwitness_rdagent campaign-inspect --request "$RESEARCH_OUTPUT/request.json"
python -m quantwitness_rdagent campaign-resume --request "$RESEARCH_OUTPUT/request.json"
```

准备生成Parquet、Catalog、来源材料和完整包。
请求使用research_kind=package。
候选只覆盖基包已有且类型兼容的节点参数。
输入、开发范围和指标语义在会话开始时冻结。

outcome.json保存在session目录。
selected_result_ref指向选定开发Result。
恢复继续原execution，不重算已发布结果。
技术中断保留预约，诊断保存在候选目录。
最终test/holdout不能作为自动循环反馈。

## Qlib参数示例

安装核心ml依赖后替换准备命令。

```bash
python integrations/rdagent/examples/package_campaign/prepare_qlib.py --output "$RESEARCH_OUTPUT"
```

示例使用10只虚构ETF和79个价格会话。
两个Ridge候选的正则化强度为0.1和1.0。
各候选独立训练，封存模型、处理器和Result。
目标是相同validation样本的均方误差。
开发图显式声明evaluation_scope=development。
最终selection与locked-holdout留在独立研究中。
