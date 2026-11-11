# 两轮预测校准

本例包含16条虚构开发预测。
原预测为标签的2倍。
第1轮评价原预测，第2轮采用0.5缩放系数。
预期开发均方误差从0.0000255变为0。
教学输入不具有正式Result身份。

## 运行

前置条件：已安装可选集成和固定RD-Agent。
在公开源码根执行命令。
WORK指向仓库外目录。

```bash
python integrations/rdagent/examples/prediction_campaign/prepare.py --output "$WORK/prediction"
python -m quantwitness_rdagent campaign-run --request "$WORK/prediction/request.json"
python -m quantwitness_rdagent campaign-inspect --request "$WORK/prediction/request.json"
python -m quantwitness_rdagent campaign-resume --request "$WORK/prediction/request.json"
```

准备只使用Python标准库。
实际调度使用RD-Agent LoopBase。
固定反馈策略不调用模型或数据库。
恢复保留原轮次收据，不增加评价。
输出保存冻结请求、输入、提案、评价和outcome.json。
正式包执行见[参数研究](../package_campaign/README.md)。
