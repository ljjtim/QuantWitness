# 从研报规格到已验证因子

本场景把指定 PDF 页面的规则提取为待审阅规格，经人工确认后交给 RD-Agent 生成 `compute.py`，由 RP 完成准入、执行、独立验证和报告。Qlib 负责已接入的机器学习组件与模型图表；本例的固定因子计算不经过模型训练。

## 准备材料与环境

使用本集成的完整源码，按[安装说明](../README.md#安装与入口)准备固定版本 RD-Agent 和 Linux 调度环境。可选包要求 Python 3.11 及以上；Windows 模型客户端另装 `live` 依赖，提取 PDF 的环境另装 `materials` 依赖。核心 RP 必须在工作环境可导入。当前已有本机验收，不等同于全新机器安装验收。

开始前需要完整的 ResearchPackage、来源归档、输入快照清单、归档 Catalog Lock、固定 adapter、项目算子声明和独立 Verifier。冻结请求字段见[请求说明](../README.md#冻结请求)。这些输入须按具体研究准备；本入口不会从一篇论文自动补齐研究包与行情。

目录、环境、缓存及运行产物放研究磁盘。下文使用 `I:/research` 和 WSL 的 `/mnt/i/research` 示例，替换为自己的位置。模型、接口地址、密钥和代理只保存在仓库外 `.env`，字段见安装说明；不使用 `export` 或 `setx` 配置它们。

## 指定页面与人工确认

先人工选择包含公式定义的页面。提取命令会把所选页面文本和题录发给配置的模型，产生调用费用；执行前确定材料使用范围及调用预算。`--output` 指向新目录，`source-id` 必须对应研究包声明的来源。

```powershell
python -m quantwitness_rdagent spec-extract --package I:/research/study/package --source-archive-root I:/research/sources --source-id paper --pages 4 5 6 --output I:/research/paper-spec --model-env-file I:/research-private/model.env --max-calls 1 --max-output-tokens 8192
python -m quantwitness_rdagent spec-review --draft I:/research/paper-spec/draft.json --materials I:/research/paper-spec/materials.json --output I:/research/paper-spec/review.md
```

逐条核对 `review.md`、原文和 `materials.json` 中的定位。`draft.json` 保留模型草稿；另写 `decisions.json`，包括全部规则 ID、全部歧义的决定与理由、接口和审阅结论，结构见[场景说明](../README.md)。区分原文明示、推断和项目决定。确认时特别核对交易 bar 网格、午休与跨日、缺失值、窗口端点、标准差口径及信号何时可用。

获得具体决定的批准后再执行确认。`--approve` 表示记录已有批准，不授予代理自行批准的权力。

```powershell
python -m quantwitness_rdagent spec-confirm --draft I:/research/paper-spec/draft.json --materials I:/research/paper-spec/materials.json --decisions I:/research/paper-spec/decisions.json --confirmed-by reviewer --approve --output I:/research/paper-spec/confirmation.json
python -m quantwitness_rdagent spec-render --draft I:/research/paper-spec/draft.json --materials I:/research/paper-spec/materials.json --decisions I:/research/paper-spec/decisions.json --confirmation I:/research/paper-spec/confirmation.json --output I:/research/paper-spec/formula.txt
```

确认保存材料、规格和决定的完整正文。后续修改这些内容需要重新确认；原提取记录的待确认状态保留，以 `confirmation.json` 与当前渲染检查判断确认是否有效。

## 生成代码与正式评价

通过`request-build`把已确认规格放入一份新请求。先写显式模板，保留输入、独立Verifier、路径、预算和资源；live模板声明模型及接口地址，可省略由确认提供的`formula/interface`。完整公式进入`confirmed_formula`，live提示使用同一正文；固定响应也可绑定确认规格。

```powershell
python -m quantwitness_rdagent request-build --template I:/research/request-template.json --draft I:/research/paper-spec/draft.json --materials I:/research/paper-spec/materials.json --decisions I:/research/paper-spec/decisions.json --confirmation I:/research/paper-spec/confirmation.json --output I:/research/request.json
```

构建命令重验来源与确认，拒绝覆盖已有输出，不读取模型配置、不启动会话。新请求使用新会话目录；保留旧请求及原始模型响应。模板字段见[请求说明](../README.md#冻结请求)。不同公式通过`formula_evaluation`显式绑定自身Verifier及覆盖表。

公式生成只允许纯计算，固定 adapter、输入、评价范围、资源和 Verifier 不由模型修改。先在覆盖缺失、窗口及交易时序的合成输入中完成正式验证，再按已批准的真实范围执行同一候选源码。真实重算需要相应执行批准，预算不能从历史会话自动延续。

在已配置的 Linux 调度环境执行：

```bash
"$RD_AGENT_PYTHON" -m quantwitness_rdagent run --request /mnt/i/research/request.json --model-env-file /mnt/i/research-private/model.env
```

此命令会调用模型并委托 RP 执行研究。当前限制为一个外层循环、一个并行任务、最多三次编码尝试；技术错误可触发受限修复。模型不能根据收益排名自行修改研究方向，最终 holdout 不能进入修复反馈。

## 结果与恢复

查看现有状态不调用模型，也不启动研究：

```bash
"$RD_AGENT_PYTHON" -m quantwitness_rdagent inspect --request /mnt/i/research/request.json
```

按 `session/outcome.json` 中的实际路径读取 Result、独立 VerificationResult 和报告。分别确认执行和验证状态；命令返回成功不等于验证通过。`model-calls/call-NNNN.json` 保存原始响应及调用用量，候选目录保存对应源码。保留材料、确认、请求、候选和结果的来源关联。

恢复有副作用，可能继续未完成计算或使用剩余获准调用预算：

```bash
"$RD_AGENT_PYTHON" -m quantwitness_rdagent resume --request /mnt/i/research/request.json --model-env-file /mnt/i/research-private/model.env
```

恢复使用原冻结请求与原会话。已完成响应直接复用，调用预算不重置；`reserved` 或 `failed` 调用先核实状态，不自动重复付费。基础设施故障沿用原候选和 RP 诊断处理。输入、公式、模型身份或资源变更使用新请求，不能编辑历史会话绕过冻结规则。

## 已验收案例

“待著而救”案例完成指定第4—6页提取、13项人工决定、确认规格生成代码、23项边界样例、合成正式验证及同一源码的真实归档验证。真实输入为2525只证券全集、117个交易 session、69465600行分钟；八节点成功，联合 Verifier 3.0.0 与21类表对照通过。15150条月末记录与固定公式基线的最大绝对差为8.88e-16。

这条材料到真实结果链使用两次模型调用、23876 token；真实归档回放没有新增模型调用。公开源码不附带研报原件、行情、私密配置及本地 Result，使用者须提供自己的材料与输入。

公开的第二公式入口见[成交量集中度示例](../examples/volume_concentration/README.md)。它用虚构分钟行情演示另一公式的准备、材料审阅、请求构建、固定响应执行和恢复；`live_llm_calls=0`，不需要模型密钥或真实行情。

## 已知限制

- 仅处理指定页面文本；空文本页阻断，公式图片、图表、阅读顺序须人工核对，没有 OCR 或整篇自动找齐公式的能力。
- 当前是固定接口下的公式复现和技术修复，未提供自主假设、多轮研究决策或任意 Python 程序执行。
- 独立原始分钟参考覆盖两只指定证券，不代表全市场逐分钟独立复算；真实验收结论为 `research_observation`。
- 论文发表时间、历史样本期、数据修订可见性分别记录。历史样本复现不证明当时已知该方法，也不证明长期盈利。
