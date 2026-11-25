# 实现身份的影响范围

判断哪些源码和环境变化会影响恢复。

## 开始前

- 输入：算子入口、显式依赖和冻结环境记录。
- 输出：节点实现身份与恢复允许范围。
- 命令中的尖括号内容是待替换参数。

## 使用顺序

1. 查看入口和 `dependency_modules`。
2. 按真实执行依赖判断影响范围。
3. 实现或输入变化后生成新计划。
4. 让长任务使用保持不变的安装环境。

实现身份由算子声明的模块、显式依赖模块和整文件源码字节构成。源码统一 CRLF/LF 后计算现有内容身份。不扫描相邻文件、项目目录或整个源码包，也不按单个函数截取 AST。

## 源码身份、恢复和跨运行缓存

`runtime/operator_definition_factory.py` 的 `_implementation_code_hash()` 使用入口模块与 `dependency_modules` 计算身份。同一进程复用文件版本一致的模块摘要。`_runtime_adapter_ref()` 合并共享 helper、适配器族依赖及算子实现模块，形成 `OperatorRuntimeAdapterRef`。内建算子注册从当前真实源码计算代码摘要并随进程缓存。节点执行和恢复核对实际使用的适配器及其闭包。

修改已登记算子的实现后，下一次运行会按当前源码动态重算新的实现与适配器摘要，使旧实现身份的 checkpoint 失效，无需手动回写固定哈希清单。

| 变化 | 局部源码身份 | 运行约束 |
| --- | --- | --- |
| 清单之外的相邻文件变化 | 不变 | 仍须满足节点原有环境画像 |
| 入口、显式共享依赖或显式动态依赖变化 | 改变 | 旧实现身份的 checkpoint 被拒绝 |
| 上游实现变化、输出内容相同 | 不直接传播到下游实现身份 | 下游仍复验输入 ArtifactRef 和自己的节点合同 |
| 构建制品或已记录依赖变化 | 由完整审计环境记录 | 严格审计绑定构建制品与全部依赖。普通缓存使用相应的运行时与依赖兼容条件 |

普通跨运行复用使用节点投影 `node-local-v5`。核心内置算子在普通语义/数值缓存模式下，将实现缓存身份绑定为 `FRAMEWORK_SEMANTIC_VERSION`、`implementation_id` 与 `semantic_implementation_code_hash(implementation)`（由 `platform/source_identity.py` 的 `semantic_python_source_bytes()` 剥离注释与无关空行后计算），同时继续把 `runtime_adapter_ref` 和 `build_artifact_digest` 隔离在缓存键之外。因此只修改注释、空行或未接入核心规则的日志文案时可继续命中语义缓存；修改真实计算逻辑则仅使受影响算子缓存失效。`audit_implementation_digest`、`research.model.locked-holdout` 以及 `--strict-audit` / `QUANTWITNESS_FROZEN_CACHE_AUDIT=1` 仍保持整文件字节级校验（`BYTE_EXACT`）。环境合同继续绑定 Python 运行时、平台和已记录依赖，数值算子使用它声明的数值后端，持久系列及留出消费规则保持原合同。

原生 LEAN Python 策略的 bundle 封存原始源码及文件摘要，同时记录源码语义摘要。语义摘要覆盖 bundle 内每个 Python 文件的 AST，包括导入的本地模块，以及其他随包文件的原始身份。注释和空行变化保持语义摘要相同。改变可执行语句，包括日志调用或参数表达式，会改变策略身份。重新构建旧的 Python 策略 bundle 以生成语义摘要。严格审计使用原始 bundle 和 LEAN 安装锁。

缓存规则仍要求当前计划完成准入，并以显式指定的来源运行作为复用依据。实际引用的请求、输入、参数、时钟、种子与输出工件均继续按原合同核对。恢复仍使用原计划并复验 registry、definition、环境与工件。

## 共享实现的实际边界

当前清单按执行文件和适配器族划分。可以直接确认的共享关系有：

- `runtime/adapters/model.py` 中的 `research.model.fit.v2` 已拆入独立计算内核 `runtime/model_fit_execution.py`，并与其余 Walk-forward 算子通过 `runtime/model_artifact_io.py` 共享元数据与列式读写；`runtime/walk_forward_model_execution.py` 保留切分、预测、评估、选择与留出实现（并重导出兼容性符号）。修改下游预测、评价、选择或留出逻辑不再使已完成的高耗时 `model.fit` 检查点失效。
- `runtime/identity.py` 的 `project_worker_adapter_hash(manifest)` 按项目算子能力范围裁剪哈希输入：非 LEAN 项目算子不再纳入 `lean_*` helper 与 LEAN 合同文件；未声明 `causal_plan` 参数的非因果项目算子不再纳入 `project_causal*` helper 与 `project_causal_contract.py` / `causal_time.py`。
- `runtime/adapters/minute_io.py` 同时承载分钟扫描、重采样、分区和行情交接 helper。使用它的适配器保留 `_MINUTE_IO_DEPENDENCIES`。
- `runtime/adapters/common.py` 使用 `merge_research_data_bundles`、`CorporateAction.from_dict()` 和研究语义对象。其文件摘要与显式共享依赖进入适配器身份。

这些文件会放大部分局部编辑的影响范围。但不能仅凭某个节点当前没有选择某条分支就移除该分支的源码身份。继续缩小范围需要按执行职责拆分共享源码文件，并同时审阅导出、实际调用和公共定义。仅缩短身份清单不能完成这项工作。

## 长任务的依赖冻结

已有 `BuildManifest`、v2 dependency distribution lock、`AuditEnvironmentManifest` 及不可变的实现引用可记录构建和依赖身份。当前应用服务 `application/research_run.py` 的 `_build_runtime_audit_environment()` 从算子 manifest、`execution_service.py` 和 `resource_governor.py` 构建运行摘要，并记录 NumPy、pandas、PyArrow、DuckDB 的已安装分发身份。该路径不等同于直接加载发行时的 `BuildManifest`。

正式运行通过已构建 manifest 保存算子引用。节点开始执行前重新计算 Runtime adapter 源码身份，变化时拒绝继续执行。现有发布路径提供 wheel、sdist、构建清单和依赖锁，参见[安装与发布](release.md)和[运行与恢复](runtime.md)。

长任务可使用独立安装且运行期间保持不变的 wheel 环境，研究与恢复使用相同构建与依赖锁。该做法复用现有发行路径。共享源码目录中的可编辑安装不能视为物理冻结环境。

## 框架测试与项目接线

准入保存 DAG、算子图和各查询文件时，用写出的字节生成冻结文件清单，并记录本计划实际使用的算子合同。普通运行读取这些文件时同时核对清单，直接执行冻结 DAG。严格审计还会从研究声明重建图和 DAG，检查准入语义。注册其他算子不会改变已有节点的实际依赖。

框架的 `tests/test_admission_compilation_reuse.py` 使用合成研究包、QueryIR 和 registry。用例验证重复编译仍通过准入门禁，且查询保持一致。用例还验证平台身份注入保留冻结参数，以及最终 registry 拒绝向外传播。

`tests/test_implementation_identity_scope.py` 使用临时 Python 文件及普通 checkpoint 文件。验证无关文件变化不改变身份、实际入口及共享依赖变化拒绝旧 checkpoint、显式动态依赖变化拒绝旧 checkpoint。另扫描框架测试中的日期项目路径字面量，阻止同类反向加载路径重新进入框架测试。

## 已知边界

- 显式清单不自动推导完整传递导入图。新增实际执行依赖时在对应入口登记并审阅。没有生产消费者的依赖不保留在运行身份中。
- 身份记录和 manifest 缓存没有自动复制、隔离核心执行源码。也没有禁止协作者修改运行中的可编辑安装。当前执行前漂移检查会拒绝变化，不能保证共享开发目录中的长任务持续执行。
## 有界类型编码

类型身份编码按固定额度合并宽标量 mapping 的 token，再更新摘要。编码保留原有键排序、类型标签、浮点 hex 和 UTF-8 字节。快路径最多256项，单批字符缓冲最多16384。

复杂嵌套、长标量及
超限mapping继续沿原流式路径。该优化不改变codec版本或已编码对象的摘要。


## 常用名称

| 名称 | 含义 |
| --- | --- |
| ResearchPackage | 数据、算法、样本、指标和资源的完整研究声明 |
| Catalog Lock | 已确认的数据字段、来源和可见性说明 |
| PIT（按时点可见） | 在过去某时刻只使用当时已知的信息 |
| QueryIR | 限定字段、范围、排序和预算的数据请求 |
| DAG（计算步骤图） | 节点之间的先后依赖 |
| Artifact（工件） | 带类型、内容身份和来源引用的产物 |
| checkpoint（检查点） | 通过正式验收的节点输出及恢复依据 |
| Result（正式结果包） | 正式表、运行摘要和配置、实现、输入引用 |
| holdout（最终留出集） | 开发选择之外的最终评价区间 |
| Supervisor | 监督执行、发布检查点和记录事件的主进程 |
| Worker | 执行节点计算的独立进程 |
| manifest（清单） | 记录文件、类型、范围和内容身份的文件 |
| schema（结构） | 表的字段名称、类型和约束 |
