# 保存模型，让预测可以重现

## 开始前

- 完成受支持 Qlib 模型的训练。
- 在 `ResultSpec` 中选择模型索引表。
- 保留与模型相符的依赖环境。
- 只加载可信本地模型。

模型索引表说明每个模型保存在哪里。
处理器保存训练期学到的标准化和缺失值规则。
正式结果包（Result）同时保存表和支持文件。

## 读取顺序

1. 用 `ResultStore.open_snapshot` 打开正式结果包。
2. 读取模型索引表。
3. 按 `artifact_key` 和 `source_path` 读取支持文件。
4. 准备与封存顺序一致的特征输入。
5. 用 `predict_bundle` 执行显式预测。

字节校验只检查内容。显式预测才加载模型和处理器。
模型索引表未进入 `ResultSpec` 时，模型仍在原运行工件中。

训练结束后，光保存一个指标不够。还需要模型文件、数据处理器、特征顺序和训练配置。它们一起决定恢复后是否还能产生相同预测。下面说明模型随结果包交付和检查的方式。

## 处理规则

序列 split 在同次执行中只构建 1 次完整基础 Feature context。窗口资格计算使用局部副本。标签连接复用基础表。原有历史范围、PIT 和来源校验继续生效。

同次 fit 按 fold 调度候选，同一进程最多保留一个 fold 的已验证序列窗口绑定。候选处理器与模型仍分别训练，跨 fold、worker 或运行重新绑定。validation 预测也只在当前 fold 复用绑定。缓存的四张表纳入节点预算，切换 fold 后释放，正式输出仍按冻结候选与 fold 顺序归并。

序列读取按当前样本批或每日横截面批向量化取窗。读取复用 Qlib 底层索引与填补政策。系统不展开全历史稠密窗口。

样本顺序、重复索引、尾批及权重保持原生含义。训练、预测和早停仍使用上游模型实现。

评价逐列检查特征类型和无穷值，只物化标签矩阵并按封存顺序执行 learn 处理器。PIT、样本身份、标签有限值与返回顺序沿用原合同。fold 成员通过同次样本索引定位，索引及查找引擎在消费期间持续计入预算。并行 fit 在容量计算和启动 worker 前释放父进程的闲置数据。释放范围包括完整样本、序列窗口和位置表。子进程仍独立验证冻结输入。

ResultSpec 选中 `research.qlib-model-inventory.v1` 模型索引表时，Result 同时封存成功模型的配置、模型文件和处理器状态。模型索引表来自 fit 或 holdout 输出。每份 Result 的 schema_id 保持唯一。

索引表成功行使用 `status=fitted`，并声明 `config_path` 与 `model_path`。配置中的 `model_path` 必须与索引表一致，`processor_files` 按 `infer`、`learn` 列出处理器相对路径。所有路径相对该模型工件根目录。

只封存清单明确引用且已经提交的文件，失败候选不交付模型。未在 ResultSpec 中选择模型索引表时，模型文件留在运行工件中。

模型和处理器使用现有 ResultSupportFile，位于 `support/<artifact_key>/<source_path>`。其摘要沿用 ExternalArtifactStore 的提交记录，不另建模型存储。Result 发布后可独立校验模型字节，不需要原运行目录，也不会反序列化或执行模型。

读取模型文件时，从模型索引表的 ResultTableManifest 取得 `artifact_key`，再调用 `ResultStore.read_support_bytes(bundle, artifact_key=..., source_path=...)`。多个工件可能使用相同文件名，不能仅按文件名选择首项。模型加载与预测仍须使用受支持的 Qlib 模型及相符环境。

显式恢复预测前，用 `ResultStore.open_snapshot` 校验 Result。读取模型索引表后，以 `Result目录/support/<模型表artifact_key>` 作为模型根目录，将索引表对应行交给 `research.modeling.qlib.predict_bundle`。配置路径仍相对这一根目录，不需要改写配置或复制回原运行目录。推理输入只提供特征。已封存的处理器加载后不重新拟合。


## 模型证据与序列窗口

研究设计、模型配置、预测、误差和窗口由正式表及ResultSupportFile关联。最终评价的plan、prepared、opened、terminal与series.json随Result保存。JSON不复制完整序列表。

序列 bundle 和输入使用 `research.qlib-sequence-model-bundle.v2`、`research.qlib-sequence-input.v2`。共享 context 以 `context_index:int64` 定位证券会话，members 只保存 `sample_id/step/context_index`。模型子集保留研究级编号。生产窗口构建检查原始特征、时点、成员、末端资格及 raw 标签，开发用例覆盖缺失与边界。

模型加载使用相符环境和当前工件合同。历史Result和封存模型文件保持原样。输入或实现变化后重新准入，原有holdout消费记录继续有效。完整合同见[模型研究](walk_forward_model.md#正式表引用式模型事实)。

## 外部研究回测统计

项目可以在正式Result保存Qlib等开源引擎产生的研究回测统计。使用项目专属artifact/schema，明确engine及版本、信号时点、成交假设、费用、样本期和计算实现。claim限于research_observation。

此类输出不使用LEAN simulation.v2执行表类型，也不声明通过LEAN资金、T+1、权益或成交规则验收。报告以“外部研究回测统计”展示，模型分数不直接解释为预期收益。

项目算法和原文参数仍在项目bundle内，第三方实现及许可、实际配置和结果引用随研究保存。已有模型、留出账本和结果发布规则不因增加外部统计而改变。实际交易研究继续使用LEAN正式路径。

显式预测资格的测试输出可包含未成熟Label：prediction仍为有限模型输出，actual、raw_label、evaluation_label为null，未知Label时间为null。IC、损失及分组统计只对finite实际标签计算，不能为评分删除同日其他预测或改变GAT图节点。交易消费者可使用全部合格预测。

整个测试fold无成熟标签时，test指标为null且sample_count=0，不作为零分登记。资格输入与fold边界见[滚动模型](walk_forward_model.md#无成熟标签的预测资格)。

## 错误处理

| 现象 | 处理方式 |
| --- | --- |
| 模型索引表缺失 | 检查 ResultSpec 是否选择模型索引表。 |
| 支持文件缺失或校验失败 | 恢复完整正式结果包。 |
| 恢复预测环境不符 | 使用封存配置要求的依赖和设备。 |
