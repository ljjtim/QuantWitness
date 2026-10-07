# Qlib 模型交付

ResultSpec 选中 `research.qlib-model-inventory.v1` 模型索引表时，Result 同时封存成功模型的配置、模型文件和处理器状态。模型索引表来自 fit 或 holdout 输出；每份 Result 的 schema_id 保持唯一。

索引表成功行使用 `status=fitted`，并声明 `config_path` 与 `model_path`。配置中的 `model_path` 必须与索引表一致，`processor_files` 按 `infer`、`learn` 列出处理器相对路径。所有路径相对该模型工件根目录；只封存清单明确引用且已经提交的文件，失败候选不交付模型。未在 ResultSpec 中选择模型索引表时，模型文件留在运行工件中。

模型和处理器使用现有 ResultSupportFile，位于 `support/<artifact_key>/<source_path>`。其摘要沿用 ExternalArtifactStore 的提交记录，不另建模型存储。Result 发布后可独立校验模型字节，不需要原运行目录，也不会反序列化或执行模型。

读取模型文件时，从模型索引表的 ResultTableManifest 取得 `artifact_key`，再调用 `ResultStore.read_support_bytes(bundle, artifact_key=..., source_path=...)`。多个工件可能使用相同文件名，不能仅按文件名选择首项。模型加载与预测仍须使用受支持的 Qlib 模型及相符环境。

显式恢复预测前，先由 `ResultStore.open_snapshot` 校验 Result 并读取模型索引表，再以 `Result目录/support/<模型表artifact_key>` 作为模型根目录，将索引表对应行交给 `research.modeling.qlib.predict_bundle`。配置路径仍相对这一根目录，不需要改写配置或复制回原运行目录。推理输入只提供特征；已封存的处理器加载后不重新拟合。
