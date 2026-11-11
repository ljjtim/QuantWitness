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

## 源码身份与恢复身份

`runtime/operator_definition_factory.py` 的 `_implementation_code_hash()` 读取入口模块与 `dependency_modules`。`_runtime_adapter_ref()` 合并共享 helper、适配器族依赖及算子实现模块，形成 `OperatorRuntimeAdapterRef`。`runtime/operator_definitions.py` 的 `production_runtime_adapter_code_hash()` 按该引用的同一清单重新读取当前源码。

| 变化 | 局部源码身份 | 恢复的额外约束 |
| --- | --- | --- |
| 清单之外的相邻文件变化 | 不变 | 仍须满足节点原有环境画像 |
| 入口、显式共享依赖或显式动态依赖变化 | 改变 | 旧实现身份的 checkpoint 被拒绝 |
| 上游实现变化、输出内容相同 | 不直接传播到下游实现身份 | 下游仍复验输入 ArtifactRef 和自己的节点合同 |
| 构建制品或已记录依赖变化 | 不改变本页的局部源码身份 | `byte_exact` 仍绑定整构建制品和全部依赖。`numerical` 绑定声明的数值后端 |

`runtime/identity.py` 的 `CacheCompatibilityProfile.from_audit()` 保留这两类环境合同。无关源码不改变局部身份，并不意味着同一旧计划一定可恢复。registry、definition、输入、固定时钟和种子、checkpoint、typed 输出及 ExternalArtifact 仍各自复验。公共定义变化仍需重新审阅和准入。

## 共享实现的实际边界

当前清单按执行文件和适配器族划分。可以直接确认的共享关系有：

- `runtime/adapters/model.py` 的六个入口调用同一个 `runtime/walk_forward_model_execution.py`。拟合、预测和留出评价还要保留候选模型可能选择的 Qlib、序列模型及生成模型实现。
- `runtime/adapters/minute_io.py` 同时承载分钟扫描、重采样、分区和行情交接 helper。使用它的适配器保留 `_MINUTE_IO_DEPENDENCIES`。
- `runtime/adapters/common.py` 使用 `merge_research_data_bundles`、`CorporateAction.from_dict()` 和研究语义对象。其文件摘要与显式共享依赖进入适配器身份。

这些文件会放大部分局部编辑的影响范围。但不能仅凭某个节点当前没有选择某条分支就移除该分支的源码身份。继续缩小范围需要按执行职责拆分共享源码文件，并同时审阅导出、实际调用和公共定义。仅缩短身份清单不能完成这项工作。

## 长任务的依赖冻结

已有 `BuildManifest`、v2 dependency distribution lock、`AuditEnvironmentManifest` 及不可变的实现引用可记录构建和依赖身份。当前应用服务 `application/research_run.py` 的 `_build_runtime_audit_environment()` 从算子 manifest、`execution_service.py` 和 `resource_governor.py` 构建运行摘要，并记录 NumPy、pandas、PyArrow、DuckDB 的已安装分发身份。该路径不等同于直接加载发行时的 `BuildManifest`。

正式运行通过已构建 manifest 保存算子引用。节点开始执行前重新计算 Runtime adapter 源码身份，变化时拒绝继续执行。现有发布路径提供 wheel、sdist、构建清单和依赖锁，参见[安装与发布](release.md)和[运行与恢复](runtime.md)。

长任务可使用独立安装且运行期间保持不变的 wheel 环境，研究与恢复使用相同构建与依赖锁。该做法复用现有发行路径。共享源码目录中的可编辑安装不能视为物理冻结环境。

## 框架测试与项目接线

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
