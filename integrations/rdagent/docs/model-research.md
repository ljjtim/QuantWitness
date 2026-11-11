# 模型结构研究

模型研究保留特征、标签、处理器和训练范围。
提案只改变受支持的前馈网络结构。
每轮使用正式开发Result评价结构。
最终test/holdout不进入提案或反思。

## 声明请求

请求使用`rd-model-research-v2`。
`package_template`只含1个固定基准。
`confirmed_spec`指向项目模型定义文件。
`baseline`声明网络定义、假设和依据。
`training`固定epochs、learning_rate、early_stop和l2。

网络定义使用`definition.nodes`。节点数为1至8（含）。每个节点声明inputs、width和activation。

`-1`表示原输入，其余编号指向先前节点。宽度为1至64（含）。激活函数为identity、relu、tanh或gelu。

末节点宽度为1，激活函数为identity。全部节点必须参与输出。

```json
{"nodes": [
  {"inputs": [-1], "width": 4, "activation": "tanh"},
  {"inputs": [-1, 0], "width": 1, "activation": "identity"}
]}
```

预算包含轮数、模型调用数和输出token额度。
`repairs`限制结构修复次数。
失败候选消耗已预约预算。
失败诊断不携带金融指标。

## 运行示例

前置条件：已安装核心ml依赖、固定RD-Agent和可选集成。
从公开源码根执行命令。
`WORK`指向仓库外目录。

```bash
python integrations/rdagent/examples/model_research/prepare.py --output "$WORK/model"
python -m quantwitness_rdagent model-run --request "$WORK/model/request.json"
python -m quantwitness_rdagent model-inspect --request "$WORK/model/request.json"
python -m quantwitness_rdagent model-resume --request "$WORK/model/request.json"
```

示例使用合成行情和固定响应。
准备命令不训练模型。
正式执行保存处理器、网络源码、权重和预测。
生成模型沿用Qlib生产入口。

## 经验与恢复

`coding_knowledge.source_sessions`引用正式开发会话。
`model_family`限定可复用的模型族。
编译经验只反馈技术事实。
最终留出指标不进入经验上下文。
使用方式见[研究知识](coding-knowledge.md)。

恢复必须使用原请求和原材料。
完成调用从收据复用。
改变训练设置、输入或模型身份时创建新会话。
联合研究显式绑定因子空间和特征槽。
绑定方式见[联合研究](joint-research.md)。
