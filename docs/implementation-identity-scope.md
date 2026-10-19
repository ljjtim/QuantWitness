# 实现身份的影响范围

实现身份由算子声明的模块、显式依赖模块和整文件源码字节构成。源码统一 CRLF/LF 后计算现有内容身份；不扫描相邻文件、项目目录或整个源码包，也不按单个函数截取 AST。

## 源码身份与恢复身份

`runtime/operator_definition_factory.py` 的 `_implementation_code_hash()` 读取入口模块与 `dependency_modules`。`_runtime_adapter_ref()` 合并共享 helper、适配器族依赖及算子实现模块，形成 `OperatorRuntimeAdapterRef`。`runtime/operator_definitions.py` 的 `production_runtime_adapter_code_hash()` 按该引用的同一清单重新读取当前源码。

| 变化 | 局部源码身份 | 恢复的额外约束 |
| --- | --- | --- |
| 清单之外的相邻文件变化 | 不变 | 仍须满足节点原有环境画像 |
| 入口、显式共享依赖或显式动态依赖变化 | 改变 | 旧实现身份的 checkpoint 被拒绝 |
| 上游实现变化、输出内容相同 | 不直接传播到下游实现身份 | 下游仍复验输入 ArtifactRef 和自己的节点合同 |
| 构建制品或已记录依赖变化 | 不改变上述局部源码身份 | `byte_exact` 仍绑定整构建制品和全部依赖；`numerical` 绑定声明的数值后端 |

`runtime/identity.py` 的 `CacheCompatibilityProfile.from_audit()` 保留这两类环境合同。无关源码不改变局部身份，并不意味着同一旧计划一定可恢复；registry、definition、输入、固定时钟和种子、checkpoint、typed 输出及 ExternalArtifact 仍各自复验。公共定义变化仍需重新审阅和准入。

## 共享实现的实际边界

当前清单按执行文件和适配器族划分。可以直接确认的共享关系有：

- `runtime/adapters/model.py` 的六个入口调用同一个 `runtime/walk_forward_model_execution.py`；拟合、预测和留出评价还要保留候选模型可能选择的 Qlib、序列模型及生成模型实现。
- `runtime/adapters/minute_io.py` 同时承载分钟扫描、重采样、分区和行情交接 helper；使用它的适配器保留 `_MINUTE_IO_DEPENDENCIES`。
- `runtime/adapters/common.py` 使用 `merge_research_data_bundles`、`CorporateAction.from_dict()` 和研究语义对象；其文件摘要与显式共享依赖进入适配器身份。
- `runtime/adapters/validity.py` 同时包含 Catalog admission 与数据观察入口，后者调用 `operator_graph_evidence.py`。共享文件中的其他入口变化也会改变该族的整文件身份。

这些文件会放大部分局部编辑的影响范围，但不能仅凭某个节点当前没有选择某条分支就移除该分支的源码身份。继续缩小范围需要按执行职责拆分共享源码文件，并同时审阅导出、实际调用和公共定义；仅缩短身份清单不能完成这项工作。

## 长任务的依赖冻结

已有 `BuildManifest`、v2 dependency distribution lock、`AuditEnvironmentManifest` 及不可变的实现引用可记录构建和依赖身份。当前 CLI 的 `_build_runtime_audit_environment()` 从算子 manifest、`execution_service.py` 和 `resource_governor.py` 构建运行摘要，并记录 NumPy、pandas、PyArrow、DuckDB 的已安装分发身份；该路径不等同于直接加载发行时的 `BuildManifest`。正式运行通过已构建 manifest 保存算子引用，节点开始执行前重新计算 Runtime adapter 源码身份，变化时拒绝继续执行。现有发布路径提供 wheel、sdist、构建清单和依赖锁，参见[安装与发布](release.md)和[运行与恢复](runtime.md)。

长任务可使用独立安装且运行期间保持不变的 wheel 环境，研究、恢复和独立验证使用相同构建与依赖锁。该做法复用现有发行路径；共享源码目录中的可编辑安装不能视为物理冻结环境。

## 框架测试与项目接线

框架的 `tests/test_admission_compilation_reuse.py` 使用合成 ResearchPackage、QueryIR 和 registry，验证重复编译仍通过准入门禁、查询保持一致、平台身份注入保留冻结参数，以及最终 registry 拒绝能够向外传播。

小盘项目脚本的接线测试位于项目自己的 `tests/test_alpha_admission_compilation.py`，验证预编译查询被用于归档准入、正式编译失败不发布计划、成功时只发布正式计划。框架测试不加载项目脚本。

`tests/test_implementation_identity_scope.py` 使用临时 Python 文件及普通 checkpoint 文件，验证无关文件变化不改变身份、实际入口及共享依赖变化拒绝旧 checkpoint、显式动态依赖变化拒绝旧 checkpoint；另扫描框架测试中的日期项目路径字面量，阻止同类反向加载路径重新进入框架测试。

## 已知边界

- 显式清单不自动推导完整传递导入图。分钟有效性入口通过实现级依赖单独绑定 `evidence.minute_validity`；该模块变化只改变此入口的局部适配器身份，相邻分钟统计入口保持原清单。新增实际执行依赖时仍须在对应入口登记并审阅。
- 身份记录和 manifest 缓存没有自动复制、隔离核心执行源码，也没有禁止协作者修改运行中的可编辑安装。当前执行前漂移检查会拒绝变化，不能保证共享开发目录中的长任务持续执行。
