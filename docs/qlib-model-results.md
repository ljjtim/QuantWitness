# 保存模型，让预测可以重现

训练结束后，光保存一个指标不够。还需要模型文件、数据处理器、特征顺序和训练配置。它们一起决定恢复后是否还能产生相同预测。下面说明模型随结果包交付和检查的方式。

## 参数与行为说明

ResultSpec 选中 `research.qlib-model-inventory.v1` 模型索引表时，Result 同时封存成功模型的配置、模型文件和处理器状态。模型索引表来自 fit 或 holdout 输出；每份 Result 的 schema_id 保持唯一。

索引表成功行使用 `status=fitted`，并声明 `config_path` 与 `model_path`。配置中的 `model_path` 必须与索引表一致，`processor_files` 按 `infer`、`learn` 列出处理器相对路径。所有路径相对该模型工件根目录；只封存清单明确引用且已经提交的文件，失败候选不交付模型。未在 ResultSpec 中选择模型索引表时，模型文件留在运行工件中。

模型和处理器使用现有 ResultSupportFile，位于 `support/<artifact_key>/<source_path>`。其摘要沿用 ExternalArtifactStore 的提交记录，不另建模型存储。Result 发布后可独立校验模型字节，不需要原运行目录，也不会反序列化或执行模型。

读取模型文件时，从模型索引表的 ResultTableManifest 取得 `artifact_key`，再调用 `ResultStore.read_support_bytes(bundle, artifact_key=..., source_path=...)`。多个工件可能使用相同文件名，不能仅按文件名选择首项。模型加载与预测仍须使用受支持的 Qlib 模型及相符环境。

显式恢复预测前，先由 `ResultStore.open_snapshot` 校验 Result 并读取模型索引表，再以 `Result目录/support/<模型表artifact_key>` 作为模型根目录，将索引表对应行交给 `research.modeling.qlib.predict_bundle`。配置路径仍相对这一根目录，不需要改写配置或复制回原运行目录。推理输入只提供特征；已封存的处理器加载后不重新拟合。


## 模型证据与序列窗口

模型 facts 新写 `model_diagnostics.table_storage=result_tables_v2`，只引用正式表。设计从 study_design 读取，配置和四阶段账本从 ResultSupportFile 读取；序列窗口从正式表及已封存 Parquet 读取，JSON 不复制这些内容。

序列 bundle 和输入使用 `research.qlib-sequence-model-bundle.v2`、`research.qlib-sequence-input.v2`。共享 context 以 `context_index:int64` 定位证券会话，members 只保存 `sample_id/step/context_index`；模型子集保留研究级编号。独立验证逐窗核对原始特征、时点、成员、末端资格及 raw 标签，模型对象与权重仍不参与验证计算。

历史 v1 Result 和封存模型保持可读。旧模型显式预测使用原处理器、模型与权重；旧 plan/checkpoint 不跨新实现身份恢复，不自动迁移或重置 holdout。完整合同见[模型研究](walk_forward_model.md#正式表引用式模型事实)。
