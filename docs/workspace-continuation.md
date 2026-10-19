# Workspace 模板复用与执行接续

Workspace 把完整 ResearchPackage 交给同一条 `lint → admit → run → verify → report` 主链。新研究使用新的 execution；中断的研究通过显式 `execution_id` 接续原执行。

## 从完整声明建立研究

`workspace init --from-package` 校验并复制四份研究声明。目标目录必须不存在，原包保持不变；不提供模板时生成待填写的中性草稿。复制保留图、QueryIR、时点、种子、ResultSpec、来源、指标、claim 和金融合同，不根据研究名称合成内容。

```powershell
$template = 'I:/research/approved-study/package'
$workspace = 'I:/research/study-variant'
python -m research_pipeline workspace init $workspace --from-package $template --json
```

这里的 `$template` 必须已经是合法完整包。对应的 Catalog Lock、算子 bundle、Verifier bundle 和输入归档仍由操作者显式提供，Workspace 不生成这些材料。

### 受控参数变体

`--parameter NODE.PARAM=JSON` 只替换模板中已经存在的节点参数，可重复传入不同键。未知节点、未知参数、重复键和不兼容的类型会被拒绝；类型判定沿用 `package expand-variants`。整数可赋给浮点参数，布尔值不能冒充数字。节点、连线、请求、金融规则和 ResultSpec 不会被自动重写。JSON 对象参数替换整个对象，操作者需要提供完整对象。

以用户已准备的白银期货完整声明为例，模板中已有 `target.contracts` 参数时，可把目标手数改成两手：

```powershell
python -m research_pipeline workspace init I:/research/silver-two-contracts --from-package I:/research/approved-silver/package --parameter target.contracts=2 --json
```

本例保留原来的实际合约映射、下一交易日开盘成交、逐日结算、乘数、保证金和费用合同。参数修改会改变包身份，仍须重新 lint/admit。若策略参数同时出现在研究设计和验证声明中，必须按原包合同同步指定全部相关参数；初始化成功不代表金融合同或项目参数预检已经通过。

多变体批量生成继续使用已有 `package expand-variants`，再对每个完整输出使用 `init --from-package`。

## 三类研究怎样复用

这些示例复用已经存在的完整声明。示例路径应换成实际已经准备并通过 lint 的包目录，不表示仓库提供三种自动生成器。

| 用途 | 模板必须已经包含的合同 | 可以怎样建立变体 |
| --- | --- | --- |
| 外部特征 | 特征生产者、字段与键、可见时间、Feature 端口、模型或统计消费者、独立复核声明 | 复制完整包；只修改该模板已有的表达式或窗口参数，相关研究设计和验证参数按原合同同步 |
| 截面预测 | 证券范围、标签成熟时间、特征可见时间、时间切分、模型、预测及指标节点；使用 holdout 时还须包含其冻结边界 | 复制已经准备的 Qlib 等完整模型包，保留处理器、标签、选择和 holdout 图；参数候选仍走原 lint/admit |
| 组合配置 | 目标生成、实际持仓、市场规则、成交与费用、估值、正式结果表和独立金融验证 | 复制已有完整组合包，按其已有参数调整目标或配置；金融合同仍由正式准入和验证判断 |

```powershell
python -m research_pipeline workspace init I:/research/external-feature-variant --from-package I:/research/approved-external-feature/package --json
python -m research_pipeline workspace init I:/research/cross-section-variant --from-package I:/research/approved-cross-section-model/package --json
python -m research_pipeline workspace init I:/research/portfolio-variant --from-package I:/research/approved-portfolio/package --json
```

公开的 `examples/equity_cross_section/package` 可直接演示完整截面观察声明的复制；训练型截面预测应使用已经准备的模型包。`examples/qlib_portfolio` 和 `examples/qlib_risk` 的既有准备阶段会产生相应完整声明，之后才可作为模板。外部特征还须使用原项目已支持的实现与历史可见数据；只给一个特征名或一个 CSV 不能自动补出完整研究。

## 一条执行与接续流程

显式传入本研究批准的 Catalog 和输入。数据库与归档输入互斥，以下使用已有封存行情：

```powershell
$workspace = 'I:/research/study-variant'
$catalog = 'I:/research/inputs/catalog-lock'
$archive = 'I:/research/inputs/input-snapshot-manifest.json'
$operatorBundle = 'I:/research/bundles/operator'
$verifierBundle = 'I:/research/bundles/verifier'
$flowText = python -m research_pipeline workspace execute --workspace $workspace --catalog-lock $catalog --input-snapshot-manifest $archive --extension-bundle $operatorBundle --verifier-bundle $verifierBundle --verification-process-slots 3 --json
$flowExit = $LASTEXITCODE
$flow = ($flowText | ConvertFrom-Json).data
$executionId = $flow.execution_id
$flow
```

Workspace 与 RD-Agent 的正式包执行共用同一应用层编排。静态检查、准入、运行和独立验证仍分别执行；包文档只在当前调用内复用，不写入 plan 或 invocation。`artifact_root` 保留工件和 holdout 持久锚点职责，运行不再要求独立 handoff 输出路径。

保存返回的 `execution_id`。不传 `--execution-id` 的 execute 始终分配新 execution，适合新研究；已有执行应明确接续：

```powershell
python -m research_pipeline workspace execute --workspace $workspace --execution-id $executionId --catalog-lock $catalog --json
```

接续沿用原调用保存的输入和预算。运行中断时，统一读取 Runtime 的 inspect 建议：resume 恢复、retry-node 重试指定节点，wait 或重新准入要求不会改成新运行。正式 Result 已发布而 completion/index 尚未补齐时，从 Result 恢复导航记录，再继续原独立验证和报告。旧 invocation 按原字段和身份读取。验证内存、临时空间、scratch、Verifier bundle 和进程槽只有显式指定时才覆盖原值。运行阶段的输入、时点、种子和预算由原正式 plan/invocation 冻结，接续不能改换研究或来源。查看已有执行编号可以使用：

```powershell
python -m research_pipeline workspace inspect --workspace $workspace --json
```

| 原执行事实 | execute 接续动作 |
| --- | --- |
| plan 已发布、run 目录为空 | 复验原 plan，再启动原 execution 的 run |
| 正式 invocation 存在、尚无 Result | 委托原 `resume`；节点、checkpoint、来源和 holdout 资格由正式恢复服务判断 |
| Result 已发布 | 从正式 Result 位置复验身份，补齐完成记录和工作区索引，再继续 verify/report |
| VerificationResult 已生成 | 复验其绑定的 Result；不重复调用 verify，只补缺失报告 |
| 已通过并已有报告 | 复验独立验证产物后返回原结果，不重复研究、验证或报告 |
| 独立验证结论为 fail | 保留失败结论与报告，返回失败，不自动重新研究 |
| 缺少正式依据、产物损坏、Result 身份不符或恢复服务拒绝 | 立即拒绝，不分配新 execution，不回退重新训练 |

Result 发布后，即使尚未保存接续位置或原 run 尚未返回，也可从正式 Result 恢复运行编号、研究计划身份和结果编号，补写 `completion.json` 与工作区索引。若完成记录已写入而索引尚未更新，接续会补齐索引；重复接续保持同一身份。完成记录或索引中的已有身份与正式 Result 冲突时立即拒绝，不覆盖冲突记录。通过独立验证的结果可按原 Dashboard 门禁导出。

Runtime 已消费的 holdout 不会因 Workspace 接续得到新的运行或留出账本。所有节点成功但 Result 尚未组装时，由原 resume 处理正式组装恢复。`continuation.json` 只保存显式位置和调用参数，不能替代 plan、invocation、Result 或 VerificationResult 的正式复验。

失败 VerificationResult 只要身份与 Result 绑定有效，也可加载和生成失败报告；失败结论本身不会被当成损坏。若需要显式重验，应直接调用公共 `verify`，为本次证据指定不存在的新文件，再用公共 `report` 读取新证据：

```powershell
python -m research_pipeline verify --result $flow.result_directory --result-store $flow.result_store --output I:/research/reverification-2.json --verification-process-slots 3 --json
python -m research_pipeline report --verification-result I:/research/reverification-2.json --result-store $flow.result_store --output I:/research/reverification-2.md --json
```

重验不会覆盖原 VerificationResult。Workspace 原 execution 继续引用原证据，不自动挑选另一次验证结论。

完成判断使用 `execution_status`、`verification_status` 和 `report_path`。命令成功不能代替独立验证通过。

## 运行前资源建议

execute 在分配前输出一份 `resource_forecast`，同时写到标准错误，使长任务开始前就能看到；最终结构化输出也保留这份建议。

- `nodes`：lint 返回的真实 OperatorDefinition 资源声明，包含核心与项目节点。
- `capacity` 与 `shortages`：按正式运行服务的默认或显式容量，列出单节点声明无法容纳的维度。
- `input_archive`：只统计归档清单引用的实际文件字节，不扫描无关目录。数据库输入规模没有足够信息时为 `unknown`。
- `startup_processes`：沿用平台进程建议，列出 supervisor、worker 及 Windows 虚拟环境启动开销；声明不足会提前提示。
- `verification_copy_lower_bound`：运行前尚无 Result 时为 `unknown`。Result 发布后，先复验冻结 Verifier bundle 的授权列声明，只把未投影文件和授权支持文件实长计入确定下界。投影表的重新编码字节不计入该下界；缺少授权列声明时保持 `unknown`。
- `verification_input_size`：复用独立 Verifier 的公开尺寸算法。完整复制标为 `measured`，按授权列投影的重编码大小标为 `estimated`；超过预算的估计只作估计不足提示。完整源文件体积另列为 `verification_authorized_source_bytes`，不当作投影下界。所有尺寸均不包含验证过程额外输出或金融暂存重编码峰值。
- `peak_memory_bytes`：没有实测或可靠规模事实时为 `unknown`。
- `budgets_changed`：固定为 `false`，建议不会自动调整预算、workers、样本、seed 或金融语义。

已知不足会明确显示 `configuration_insufficient`，正式资源门禁仍负责拒绝不合格执行。需要调整时由操作者显式修改相应声明或预算。

## 已知限制

模板结构合法不代表能在任意机器或任意 Catalog 上执行；项目参数、bundle 身份、PIT 和金融合同仍须经过原主链。缺少完整声明时应补齐研究事实，Workspace 不提供任意包自动合成能力。

归档字节数、节点预算与验证复制下界分别回答输入体积、批准额度和必要复制量，不能推导任意模型或优化器的内存峰值。并行节点的组合占用和实际峰值继续由正式运行时治理。
