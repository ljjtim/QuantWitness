# 开发区预测校准：两轮研究

这个公开教学例包含16条虚构开发预测。原预测是标签的两倍；研究循环先评价基准，再根据反馈选择菜单内的0.5收缩系数。预期两轮开发MSE分别为0.0000255和0。它不是回测收益或真实市场证据。

在集成说明规定的Linux RD-Agent环境执行，`$EXAMPLE`指向本目录，`$OUTPUT`为尚不存在的输出目录：

```bash
python "$EXAMPLE/prepare.py" --output "$OUTPUT"
python -m quantwitness_rdagent campaign-run --request "$OUTPUT/request.json"
python -m quantwitness_rdagent campaign-inspect --request "$OUTPUT/request.json"
python -m quantwitness_rdagent campaign-resume --request "$OUTPUT/request.json"
```

准备过程只使用Python标准库。循环采用实际RD-Agent LoopBase；固定反馈策略不调用模型、不读.env、不访问数据库。完成恢复保留原轮次收据。输出包含冻结请求、开发输入、逐轮提议和评价、RD快照及outcome.json。

真实输入、实时假设提议、预算、恢复及数据隔离见[研究循环说明](../../docs/research-campaign.md)。合成输入明确标注fixture，不具有正式Result身份。
