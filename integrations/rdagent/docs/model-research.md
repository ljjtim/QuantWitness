# 运行时生成模型研究

`model-run` 把模型假设、结构编译、修复、正式开发评价和反思接到同一个研究会话。提案使用固定版本 RD-Agent 的 ModelHypothesisGen 与 ModelHypothesis2Experiment；评价复用 Qlib 与 QuantWitness Workspace。

## 模型范围

首批支持前馈有向无环网络。每轮声明 1 至 8 个节点，各节点宽度为 1 至 64，激活为 identity、relu、tanh 或 gelu。输入 `-1` 表示冻结特征，其他输入引用此前节点；多输入按列拼接，可以生成跳连。末节点输出一个原始收益预测。结构在运行时提出，由确定性编译器生成 `ResearchNetwork` Python 源码。

特征、标签、处理器、开发日期及训练设置在会话开始时冻结。Qlib 适配器只请求 train/valid 训练数据，预测只请求特征。模型、处理器、网络源码、数值权重及学习曲线随正式 Result 封存。独立验证从源码语法树与权重复核连接、维度和配置，并沿用模型样本与分段验证。

## 运行与恢复

按[安装说明](installation.md)准备固定上游源码和 Linux 环境，机器学习环境另需 QuantWitness 的 `ml-sequence` 依赖。完整源码根目录下执行：

```bash
python integrations/rdagent/examples/model_research/prepare.py --output /mnt/i/research/model-demo
python -m quantwitness_rdagent model-run --request /mnt/i/research/model-demo/request.json
python -m quantwitness_rdagent model-inspect --request /mnt/i/research/model-demo/request.json
python -m quantwitness_rdagent model-resume --request /mnt/i/research/model-demo/request.json
```

教学例使用合成行情和固定文本响应，不调用付费模型、不创建数据文件数据库。第二轮给出新结构，并演示一次无效连接的修复。live 使用显式 `--model-env-file` 读取 `.env`，准备与执行均需提供同一配置；凭据不进入冻结请求。

每轮正式评价在独立子进程委托同一 PackageCampaign，训练结束后释放模型内存；恢复仍使用该轮原 Workspace。

`rounds/` 保存提案、编译尝试、评价和反思；`experiments/` 保存正式 Workspace；`calls/` 保存预约预算和响应。已有响应按原请求复用，结果未知的调用停止等待核对。恢复继续原 execution，不重新建立已成功实验。每轮只向提案和反思提供独立验证通过的 validation 指标，最终 test/holdout 不进入循环。

## 编码经验

`coding_knowledge.source_sessions` 显式选择过去的模型研究会话。经验必须有实际编译诊断或通过验证的正式模型来源；成功源码读取自 Result。修复提示保留来源记录与技术反馈，不包含金融指标。可以在新会话准备时用 `--coding-source /mnt/i/research/previous/session` 引用。

## 范围限制

本入口生成受支持的前馈结构，不加载模型返回的任意 Python 插件。序列网络生成不在该入口范围。联合因子与模型方向使用[联合研究入口](joint-research.md)。固定响应验证接口、执行和恢复；模型服务上的自主提案质量需要单独的 live 验收。合成研究结果只证明流程与算法行为。
