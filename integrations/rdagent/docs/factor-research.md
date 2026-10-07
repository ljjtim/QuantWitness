# 从文档基线到新因子的研究循环

此模式使用固定 RD-Agent 的假设生成流程、实验定义模板与研究反思，调用 Qlib 表达式和原有 ResearchPackage 主链。候选在运行时产生，不要求预先列出候选菜单。当前正式案例限于日频收盘动量、均价偏离、相对波动和区间位置；方向知识只来自已验证的 development Result。

## 执行

在同时安装核心 ml 依赖、可选 RD 集成和固定 RD 源码的 Linux 环境中使用。同环境执行 Workspace；项目原公式复现的 Windows 桥不受影响。输出、模型和临时文件放在显式研究目录。

```bash
python integrations/rdagent/examples/factor_research/prepare.py --output /mnt/i/research/factor-demo
python -m quantwitness_rdagent factor-run --request /mnt/i/research/factor-demo/request.json
python -m quantwitness_rdagent factor-inspect --request /mnt/i/research/factor-demo/request.json
python -m quantwitness_rdagent factor-resume --request /mnt/i/research/factor-demo/request.json
```

完成一会话后可把正式开发记录导出为知识索引，供下一会话使用：

```bash
python -m quantwitness_rdagent knowledge-export --session /mnt/i/research/factor-demo/session --output /mnt/i/research/knowledge.json
python integrations/rdagent/examples/factor_research/prepare.py \
  --output /mnt/i/research/factor-next --campaign-id synthetic_factor_next \
  --knowledge-index /mnt/i/research/knowledge.json \
  --knowledge-output /mnt/i/research/knowledge-next.json
```

下一会话冻结索引正文和检索记录；提案必须引用记录 ID，范围外、未来、指标不一致和 test/holdout 记录会被排除。索引不是自包含结果文件，来源会话和其中的 Result、VerificationResult 必须保留。

默认准备命令创建虚构行情、教学定义和固定文本响应，不调用模型、不写数据库。运行会真实计算特征、训练 Qlib 模型并独立验证；固定文本只替代模型服务的回复，不替代正式执行或验证。因此它证明流程，不证明实时模型已自主发现投资机会。

教学请求在`package_template.source.verification_memory_bytes`显式声明`4294967296`字节（4 GiB）独立验证预算，覆盖研究组件与验证器同进程运行所需内存。此值会传给正式`verify`；其他请求省略该字段时沿用CLI默认预算。该预算属于冻结请求，恢复不得增加额度。

live 模式在 prepare 时加 `--model-env-file /mnt/i/research/private/model.env`，运行和恢复时提供同一参数。prepare 只读取公开模型身份，不发送模型请求；配置字段及代理要求沿用公式复现客户端。凭据不写入系统环境、请求 JSON 或会话快照。

## 研究步骤

1. 读取已确认定义并运行原表达式，取得独立验证的基线。
2. 反思只接收通过独立验证的开发目标、参与汇总的指标行数及方法说明（指标行数不是预测样本数）。
3. 上游假设生成和实验定义分两次请求，提出一个新表达式。
4. 新表达式写入 ResearchPackage 变体，重新执行 Qlib 训练和验证。
5. 保留正式结果与反思，下一轮提案消费这些开发历史，直到轮次或模型预算结束。

研究只修改 `historical_return`；两个既有窗口槽都使用声明表达式，`volatility`保持原定义。可执行式及含义如下，公式内各处使用相同窗口：

| 方向 | 表达式 | 窗口 |
| --- | --- | --- |
| momentum | `$close / Ref($close, N) - 1` | 1至5 |
| mean_deviation | `$close / Mean($close, N) - 1` | 1至5 |
| relative_volatility | `Std($close, N) / Mean($close, N)` | 2至5，样本标准差ddof=1 |
| range_position | `($close - Min($close, N)) / (Max($close, N) - Min($close, N))` | 2至5，最高最低相同时缺失 |

相对波动衡量价格离散程度，不是年化收益波动率；区间位置只使用收盘价，不假定有最高价和最低价字段。计算点是决策前一完整会话的收盘，不能使用当前09:30之后的行情。独立Verifier从封存价格手算这四类公式；未支持公式会被拒绝，不自动降级或扩大范围。

标准特征集与更多算子的底层用法见[Qlib表达式](../../../docs/qlib-expressions.md)，其范围大于本例已独立验证的研究范围。

## 请求与文档确认

`rd-factor-research-v1`声明：

- `campaign_id/session_root`：会话身份及目录。
- `package_template`：一个基准候选的完整开发研究包请求，来源、Catalog、归档、bundle、独立Verifier、目标及资源明确。禁止最终test/holdout计算。
- `confirmed_spec`：教学使用`kind=synthetic,path`，定义须与合成数据范围及基线一致；真实确认使用`kind=confirmed_formula`及`path/draft/materials/decisions`，调用原有`render_spec`复验确认文件。已确认的`decisions.interface`必须包含规范基线表达式，不能只指定不相关文档。
- `baseline`：`expression/hypothesis/reason`。
- `budget`：`rounds/model_calls/output_tokens/max_output_tokens_per_call`，基线计一轮；每次反思一次调用，后续每轮提案两次调用。
- `proposer`：`fixed_responses`及按请求顺序的文本文件，或`live/model/base_url`。固定响应是接口验收数据，不是可选候选菜单。

真实文档沿用`spec-extract → spec-review → spec-confirm`材料流程。确认只解决定义；实际数据来源、计算范围和模型预算仍须获准。方法含超出当前表达式或数据语义的内容时，应扩展实现并验收后再运行，不能把近似公式称为原文完整复现。

## 结果与恢复

`session/rounds/<编号>/`保存proposal、evaluation、reflection和record。`session/experiments/<候选>/`保存独立包执行，metrics记录指向正式Result和VerificationResult。`session/outcome.json`只有全部声明轮次均完成评价和反思时才为completed，否则明确incomplete和停止原因。validity通过不代表收益优于基线。

`calls/`保存不含凭据的请求和响应，并在调用前预约单次最大输出预算。恢复复用已完成响应和实验；请求内容、确认材料、基包、输入声明及bundle改变会拒绝恢复。已发送但响应未落盘的调用保留unknown，不自动重复付费；需人工核对后建立明确的新运行安排。

提案、反思均看不到整份Result、未裁剪日志、test/holdout或未验证收益。参数循环原入口`campaign-*`继续支持确定性菜单实验；`factor-*`提供新的表达式研究。

## 跨会话知识与方向

请求可增加：

```json
{"knowledge": {"index": "/mnt/i/research/knowledge.json", "output": "/mnt/i/research/knowledge-next.json", "max_records": 8}}
```

输入索引和输出索引必须使用不同路径，输出位于来源会话之外。`max_records`为1至32。导出从正式Result重新读取开发指标并核对研究包、Verifier和设计；`knowledge-input.json`保存本会话的冻结索引与入选记录。研究结束后复核并继承输入索引的全部历史记录，再追加本会话记录；检索上限只限制提示上下文，不截断历史积累。独立会话使用不同campaign_id，避免无Result的技术记录身份冲突。重复恢复不改写同一内容。原始索引改变会拒绝恢复。

检索要求指标定义、单位、方向、频率、年化政策和计算语义相同，资产来源、日历和价格口径相同；历史资产及研究会话必须包含在当前允许范围内，历史`as_of`不得晚于当前研究时点。空的`holdout_index`结构表可以存在，test/holdout评价表不能作为知识来源。提案上下文只接收已投影的开发指标、公式、反思和记录ID，不包含来源文件路径。

索引记录`baseline`、`improved`、`no_improvement`和`technical_failure`。改善按来源会话此前最佳开发指标判断；负结果仍保留正式指标，技术失败或被拒绝提案只提供技术约束，不能变成金融证据。反思是研究者对结果的解释，指标结论由正式数值决定。

有知识的新一轮增加一次方向调用，再进行两次提案调用和一次反思调用。方向支持上述四类价格因子或`stop`；方向及表达式必须引用检索到的`knowledge_refs`，表达式必须属于所选方向。重复表达式和无效引用被拒绝；没有合适知识时直接停止，不调用模型。固定教学例默认两轮、五次接口响应；预算和恢复沿用原有持久调用回执。

`knowledge-export`可导出具有最终`outcome.json`的completed或incomplete会话。暂停但尚无最终outcome的会话须先恢复。索引同时保存已验证结果与技术失败状态，但完整研究是否完成仍以outcome为准。

## 已知范围

- 上游源码固定且不修改；实际采用无副作用Scenario、正式开发结果视图和显式APIBackend。此模式不使用默认qrun、数据下载、ChatSession SQLite缓存或embedding。
- 当前知识索引只支持因子方向，不生成任意模型代码，也不进行因子/模型联合方向轮转。
- 阶段快照可恢复并复用已完成轮次；节点执行中断仍服从Runtime诊断。若inspect返回`readmit-new-run`，`factor-resume`不会强行重试，需按诊断新建运行。当前KeyboardInterrupt可能被归为该类，不能宣称任意中断均可自动恢复。
- 合成固定响应验收、live调用验收和真实市场研究是不同证据，报告必须分别说明。

## 已确认日级因子合同

待著而救日级研究显式声明 `factor_contract="dai-daily-v1"`，使用 `$dai` 基线、受限Ref/Mean派生和 `dai_following` 槽；请求字段、方向、共同资格与正式证据见[日级研究指南](dai-daily-research.md)。既有收盘价示例继续使用原合同。
