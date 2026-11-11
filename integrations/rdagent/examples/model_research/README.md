# 合成模型结构研究

本例使用虚构ETF与固定特征。
基线是单层回归网络。
后续候选包含非线性隐层与原输入跳连。
固定响应覆盖结构修复、训练和开发反思。

## 运行

前置条件见[模型研究](../../docs/model-research.md)。
在公开源码根执行命令。
WORK指向仓库外目录。

```bash
python integrations/rdagent/examples/model_research/prepare.py --output "$WORK/model"
python -m quantwitness_rdagent model-run --request "$WORK/model/request.json"
python -m quantwitness_rdagent model-inspect --request "$WORK/model/request.json"
python -m quantwitness_rdagent model-resume --request "$WORK/model/request.json"
```

准备只生成材料，不训练或调用模型。
运行保存网络源码、处理器、权重和正式Result。
请求使用rd-model-research-v2。
训练设置与开发范围随会话冻结。
最终test/holdout不进入提案或反思。
