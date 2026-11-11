# 模型研究：滚动训练与样本外评价

## 开始前

本页面向日频回归研究。先用合成示例理解流程，再接入自己的数据。

- 安装框架的 `ml` 可选依赖。
- 使用序列模型时，安装 `ml-sequence` 可选依赖。
- 准备已准入的特征、标签和交易日历。
- 为最终评价准备长期保留的研究系列目录。

特征是决策时已知的输入。标签是决策后的目标值。样本划分（fold）是一组训练、验证和测试区间。

按时点可见（PIT）表示只使用当时已知的信息。最终留出集（holdout）独立于开发选择。

## 完成一次研究

1. 运行[合成模型示例](../examples/qlib_portfolio/README.md)的 `development` 模式。
2. 检查 Result 中的预测、误差和训练范围。
3. 在开发区确定特征、候选和参数。
4. 固定研究系列和最终评价区间。
5. 运行示例的 `model` 模式。
6. 用[模型报告](qlib-report.md)查看正式结果。

`development` 适合反复比较。`model` 包含一次性最终评价。
`portfolio` 还需要 LEAN 安装和交易规则。
示例命令从公开源码根目录执行。

模型使用过去的数据训练，再使用之后的数据评价。滚动研究会沿时间推进训练和评价窗口。开发阶段用于比较候选，最终留出样本用于评价已经选定的方案。

这页说明 Qlib 模型、训练处理器、样本划分、候选选择与最终评价怎样接入同一研究流程。

## 处理规则

模型计算采用 Qlib 0.9.7 的 Dataset、Processor 与 Model，模型支持日频、每模型单期限、单标签回归。模型包括：LinearModel 的 OLS/Ridge、LGBModel、XGBModel、DEnsembleModel（样本重加权版本）以及 GRU/LSTM/Transformer/GATs及受控项目序列模型。安装使用 `pip install -e ".[ml]"`。分类、多标签和在线更新不在当前支持范围。

## 六节点主链

`split-manifest → fit → predict → fold-metrics → selection → locked-holdout`

split 只组织准入样本和角色。fit在每个candidate/fold内依次拟合Qlib Processor和Model。predict 应用冻结处理器，不再次 fit。

validation 用于早停和候选选择，属于开发区。历史 test 候选只使用当时已可见的开发区指标。最终 holdout 在配置冻结后评估，不进入调参或 AI 反馈。

切分审计按列构造，各fold共享每个样本已格式化的ISO时间，不累计逐行字典。
三个公共切分入口可接收同一 `frame_budget`，在共享时间、fold列缓冲及拼接排序分配前
检查预算。最终审计保留原字段、空值、稳定排序和manifest身份。

同一 fit worker 会话中，同 fold 的窗口只封存 1 份事实文件。后续成功序列模型的
配置引用该份context、targets与members。复用绑定同一输入、fold和输出根。

换fold、
输出根或worker后独立保存。模型及处理器仍分别封存，Result沿原配置引用收集完整依赖。

原始日线可通过[受控项目扩展](../project_extensions/README.md#中国股票日频来源)声明 `next_session_open` 时间绑定，使用冻结交易日历生成核心时间事实。模型 Label 时间列必须是带时区的时间戳。扫描边界按列时区表达同一时刻。

真实标签起止与 available_time 参与 purge、拟合和选择。split 先检查 Label row group，读取开发目标而不扫描尚未打开的 holdout 标签。最终 holdout 仍使用 prepared/opened/terminal 一次性账本，打开后的失败不会恢复访问资格。训练范围、验证角色和最终重训声明都进入证据。

## 滚动窗口与多个预测期限

开发切分支持 expanding 和 rolling。前者保持训练起点，随时间增加历史。后者按 `step_sessions` 前移固定名义训练窗。

切分日历按开发样本的 `observation_time` 日期裁切，窗口成员也按真实观察时间分组。`observation_session` 保留模型证券日期索引。前一会话收盘因子可以在下一决策会话使用，两种日期不要求相同。

生产切分按真实观察时间构造成员，purge 继续使用实际决策时点、标签结束与成熟时间。每个候选在每个 fold 单独拟合处理器与模型。实际训练成员还要经过标签结束时间、成熟时间、embargo 和完整序列资格筛选。

名义窗口不保证固定样本数。序列 warmup 可读取窗前已可见的 Feature，不把窗外标签加入拟合。

公开 Qlib 示例用 `--window-mode rolling --train-sessions 20` 声明滚动开发，用 `--horizon-sessions 5` 声明单个五会话目标，也可用 `--horizons 1 5` 一次冻结多份研究。开发模式下每个期限独立生成 ResearchPackage和Result，共享输入来源、候选与窗口配置。组清单只记录各成员结果位置，不合并不同期限的 MSE。

多期限比较使用开发指标，最终确认选择单一期限。同一研究系列的重叠留出日期禁止按期限重复打开。

标签为 `close[t+h] / close[t] - 1`，在 t+h 会话收盘结束，下一会话开盘成熟。不同期限按真实成熟时间分别 purge。冻结设计、开发样本及最终预测的期限必须一致。开发模式只封存留出边界前的输入，不开放最终留出。

滚动选项控制开发 fold。最终赢家仍以全部合格开发历史重训，末尾 validation 窗口用于早停，训练成员继续按实际标签时间 purge。最终重训不缩为最后一个固定训练窗。准备、恢复与候选声明见[公开示例](../examples/qlib_portfolio/README.md#滚动与多期限研究)。

## 候选声明

candidate_jsons 是完整 JSON 字符串列表，每项包含 candidate_id、model、processors、fit。model 使用实际支持的 Qlib class、module_path、kwargs。processors 使用 infer/learn 列表。

fit 是该模型的训练参数。CPU 模型使用单线程，有随机性的模型显式 seed。拟合节点默认声明 10 个进程槽，最终重训节点声明 4 个进程槽。

额度覆盖 Qlib 记录器和依赖辅助进程。线程数与进程槽分别约束。

```json
{"candidate_id":"ridge","model":{"class":"LinearModel","module_path":"qlib.contrib.model.linear","kwargs":{"estimator":"ridge","alpha":0.1,"fit_intercept":true,"include_valid":false}},"processors":{"infer":[{"class":"RobustZScoreNorm","kwargs":{"fields_group":"feature","clip_outlier":true}},{"class":"Fillna","kwargs":{"fields_group":"feature","fill_value":0}}],"learn":[{"class":"DropnaLabel","kwargs":{}}]},"fit":{}}
```

Linear 使用训练期拟合的稳健标准化，再填充 feature 缺失。可另选训练期拟合的 ZScoreNorm，或同一会话横截面的 CSZScoreNorm。二者只处理 infer feature，不改变训练样本资格和标签口径。

LGB/XGB 保留数值与 NaN，不强制缩放。候选默认使用原始收益标签。若另行采用 CSRankNorm(label)，分数为排名分数，原始 actual 仍独立保存供收益评价。

不能以排名分数充当预期收益率。同一搜索的候选必须采用相同的标签口径。选模指标使用 `evaluation_label`，原始收益仍保留为 `actual`。

训练窗配置不能替代实际合格样本选择：purge/不可见记录先剔除，再交 Qlib。infer 不因未来标签未知而删除行。每交易日每证券每 horizon 一行，Qlib datetime 对应 observation_session，instrument 对应 entity_id，真实带时区的可见时间另行保留。

## 模型文件与恢复

使用现有外部工件存储。models 表保存相对 bundle 路径，bundle 包含 model.pkl、已拟合 Processor 文件与 config.json。不保存 Dataset、Handler 或训练矩阵。

LGB/XGB 的 train/valid 训练曲线保存于配置和 `learning_curves` 表，Linear 与 DEnsemble 的曲线表为空。固定上游 DEnsemble 未导出曲线。DEnsemble 配置另封存子模型数量、特征顺序、权重和实际轮数，恢复预测核对实际模型状态。

FitScope 证明处理器使用的实际训练样本范围，模型可用时点另包含 validation 标签成熟时间。模型和 Processor 使用 Qlib Serializable，加载推理仅应用状态。

要交付模型的 ResultSpec 显式选择 research.qlib-model-inventory.v1 表，ResultAssembler 收集其模型、处理器和配置为 support_files。读取使用 artifact_key/source_path，不能依赖原 run-root 或同名文件的第一个匹配。完整性验证只核对字节，不执行 pickle。模型加载仅面向可信本地工件。

模型阶段使用v2输入输出合同。输入或实现变化后重新准入并新建运行，历史Result及holdout消费记录保持原样。

## 预测和报告

预测包括 candidate/fold/stage/sample_id、证券、交易日、观察/决策/可见时点、预测值和原始 actual。预测按索引映射，不依赖数组行号。未知或未成熟标签不进入训练。学习标签变换不覆盖原始经济标签。

HTML 报告用请求显式选择表、候选、阶段、fold/horizon 和日期范围，生成 Qlib 分组、IC/Rank IC、自相关。重复证券日期不自动 keep-last。算术累计收益明确标示。

模型诊断没有交易账本，不生成虚构的策略净值或成本结论。无定义 IC 或不足分组样本显示缺失说明，不填零。

## 模型有效性与交付

训练、预测和候选选择使用正式六节点合同，不能用数据行数代替预测评价。实际消费者检查切分、训练范围、validation选择、逐时点test与一次性holdout。Result保存配置、指标及四阶段账本，报告直接读取。

纯预测研究交付模型、预测和指标，不能据此声称可交易。组合研究接入正式金融节点。LEAN Result 保存实际订单、意图、日终账户、策略轨迹和指标。报告按数据与声明范围解释结论。

最终holdout成功提交后，即使下游节点失败，其访问资格仍已消费。按[严格失败节点复用](runtime.md)恢复时，所有指定节点及上游须在Worker启动前通过原身份与完整性检查。无法复用必须停止，不能回退为再次拟合或打开holdout。

## 能力范围

能力保持local_only。受支持模型、处理器和窗口仍受PIT、开发选择、最终留出及资源合同约束。程序错误不能被当作候选淘汰后静默继续。预测评价不证明策略盈利。

## 公开开发研究

[合成ETF起点](../examples/qlib_portfolio/README.md)提供开发区、最终模型和完整组合三种声明。开发区只使用训练与validation结果，研究记录和最终holdout分开。进入参数循环时，输入归档也必须限制在批准的开发截止内。

LightGBM训练记录保留节点内指标和train/valid曲线，不采集整仓Git差异。来源以RP已封存工件为准。训练记录启动或结束失败时恢复原Qlib和MLflow状态，原始训练错误保留供诊断。

## Double Ensemble 样本重加权

`qlib.contrib.model.double_ensemble.DEnsembleModel` 使用多个 LightGBM 子模型，按先前子模型的训练损失调整后继样本权重。沿用相同的 DatasetH、训练期处理器、validation 选模、test 预测和最终 holdout 重训入口。

```json
{"candidate_id":"double_ensemble","model":{"class":"DEnsembleModel","module_path":"qlib.contrib.model.double_ensemble","kwargs":{"base_model":"gbm","loss":"mse","num_models":2,"enable_sr":true,"enable_fs":false,"decay":0.9,"bins_sr":10,"epochs":100,"early_stopping_rounds":20}},"processors":{"infer":[],"learn":[{"class":"DropnaLabel","kwargs":{}}]},"fit":{}}
```

要求至少两个子模型、有限正数 decay、显式关闭特征选择。轮数和早停放在构造参数，fit 留空。CPU 单线程和根种子由 RP 注入。

可声明 sub_weights，数量须匹配子模型，非负且首项为正。模型内部每个加权前缀都必须有正权重。训练入口检查声明和范围，开发用例核对保存状态与预测恢复。

开发用例覆盖直接 Qlib 对照、保存恢复和未来标签隔离。用例还覆盖开发区拟合、validation 预测、逐时点 test 选择和一次性 holdout 重训。Double Ensemble不支持特征选择或序列输入。真实数据覆盖按项目声明。


## 序列模型与完整窗口

`research.modeling.sequence` 提供完整窗口，`research.modeling.qlib.fit_bundle/predict_bundle` 已支持显式 `sequence_context` 的 GRU/LSTM/Transformer/GATs及受控项目TS模型文件接口。固定模块为 `qlib.contrib.model.pytorch_gru_ts.GRU`、`qlib.contrib.model.pytorch_lstm_ts.LSTM` 与 `qlib.contrib.model.pytorch_transformer_ts.TransformerModel`，数据集为 `TSDatasetH`。安装使用 `pip install -e ".[ml-sequence]"`，固定Qlib 0.9.7、Torch 2.9.1。

正式 ResearchPackage 支持显式序列声明的 GRU、LSTM、TransformerModel、GATs、ProjectQlibModel 与表格混合候选。split 必须提供 `sequence_step_len>=2`，fit 和 holdout 的序列窗口长度须一致，并绑定同一对应 split。每个序列模型节点至少声明一个序列候选。

缺少或不一致的窗口在编译时拒绝。

`build_sequence_windows` 从原始 Feature 长表和显式末端构建窗口。末端只含 sample_id、证券、观察会话和 decision_time。历史上下文不依赖 Label，不携带 target。

窗口按显式冻结交易日历取同证券的连续会话，包含当前观察会话，严格保持声明的特征顺序。原有表格模型与序列窗口共用日频特征整理函数，观察时间和可见时间必须完整。

返回的 `SequenceWindows` 保存 context、targets、members、exclusions、特征顺序、日历和 step_len。members每行仅记录sample_id、step与context_index。证券、会话、观察时间、可见时间及来源引用保存在共享context中。

上市历史不足记录 `insufficient_history`，缺会话或缺少一列有效 Feature 记录 `missing_feature`，成员在末端 decision_time 后才可见记录 `feature_not_visible`。标签 purge 不删除仍可使用的历史特征。validation 和 holdout 的首个末端可以使用此前已知的历史行。

`qlib_sequence_dataset` 接收完整窗口和明确的末端分段，返回 Qlib `TSDatasetH`。训练只接受不重叠且严格先后的 train/valid，显式标签恰好覆盖这些末端。test 不接收标签，末列为 NaN 占位。

没有标签的历史行保留在上下文中，由过滤列限定哪些行成为样本末端。显式 NaN 特征可以由调用方使用冻结处理器转换。转换结果必须保持上下文索引、特征顺序，实际使用的完整窗口须全部有限。

缺会话不能由填充处理器补造。

`normalize_candidates` 对序列模型固定 `loss=mse`、`metric` 为空或 `loss`、`n_jobs=0`、`step_len>=2` 和 `complete_window`。样本批次为正整数，每日横截面批次由有效证券数决定。d_feat 可省略，由每个时点的特征数注入。

显式给出时必须相符。root_seed 注入模型，CPU侧使用单线程，结束或异常时恢复线程数及 NumPy/Torch 随机状态。

GRU/LSTM/Transformer 的训练期标准化和 Fillna 仅在合格 train 末端拟合，再应用到实际窗口上下文。当前不支持 CSZScoreNorm 特征处理器，因为它需要另行声明每个末端决策时点的可见证券集合。正式序列可使用标签CSRankNorm，原始收益独立保存，MSE评价使用相应evaluation_label。

模型文件封存完整 Qlib model、独立 weights.pt 和处理器。文件还封存 negative_mse 学习曲线。实际使用的无标签历史上下文、末端及成员事实也随模型保存。

路径使用 bundle 内相对位置，迁移目录后可继续预测。

单样本输出形状由实例级 forward hook 保持为一维，Qlib 的训练、早停和预测算法仍使用上游实现。源码副本保持原样。模型恢复核对配置、网络结构、显式权重与 model 文件中的权重，以及批次形状合同。预测只使用冻结 infer 处理器，标签末列全部 NaN，不需要传入真实 target。

`evaluate_locked_holdout` 的底层接口可提供 `development_sequence_context` 和 `holdout_sequence_loader`。最终开发模型先完成拟合，账本 opened 后才调用 holdout 数据与窗口加载器。打开后的加载失败会消费访问资格，再次调用不会重训或重新读取。

序列构建和模型消费检查原始Feature、日历、观察与可见时间、成员、末端资格和声明标签口径。开发用例从固定行情手算窗口与排除原因，预测恢复检查已保存模型和处理器。

Runtime已接通split、fit、validation、metrics、selection和holdout的序列工件。切分接口显式提供`sequence_step_len>=2`，在Feature与Label拼接前确定全部末端的完整窗口资格。所有GRU/LSTM/Transformer候选须使用同一窗口长度，混合表格候选沿用筛选后的相同样本。

缺失与晚可见末端记录到exclusions，历史Feature保留为预热上下文。工件保存context、targets、members、exclusions、日历及窗口声明，并纳入既有工件身份和节点内存预算。

最终holdout在opened之后重新读取已绑定的Feature/Label，按冻结合格末端加载标签，再构造实际预测窗口。最终开发模型仍先完成训练。开发研究的输入归档截止holdout前，图中没有selection和holdout节点，禁止把最终留出评价用于开发。

Result封存序列权重、处理器和窗口文件。训练与预测消费者检查完整窗口、末端、Label可见时间、训练子集及模型配置。开发对照覆盖窗口成员、未来Label扰动和模型恢复。重新预测使用封存模型、处理器与明确输入，不依赖测试运行目录。

公开示例通过 `--sequence-step-len` 生成开发或完整模型研究，ResultSpec包含原始Feature、Label和研究级四张序列表。示例及命令见[Qlib研究起点](../examples/qlib_portfolio/README.md)。序列研究使用当前源码、准入合同和相符依赖环境。平台安装范围见[安装与发布](release.md)。

### Transformer注意力模型

候选使用`TransformerModel`及固定TS模块。`d_model`为不小于2的偶数，`nhead`为正整数且整除`d_model`。`reg`为有限非负数。

窗口长度为2至1000，与上游位置编码容量一致。其余训练参数遵守统一序列合同。`hidden_size`不适用于该模型。

上游使用完整历史窗口内的自注意力，预测末端只读取决策时点已可见的窗口成员。窗口内的位置互相注意不引入决策时点之后的数据。模型的特征投影、位置编码、注意力头、编码器层、前馈层和优化器状态在加载时核对。

内存预算包含参数副本、固定位置编码和随窗口长度平方增长的注意力工作区。Transformer训练、早停与预测沿用Qlib，单样本输出保持一维。学习曲线继续记录negative_mse。

## 图注意力与受控项目模型

原生`GATs`使用`qlib.contrib.model.pytorch_gats_ts`。`dataset.batch_contract=daily_cross_section`按同一天的完整合格证券集合组成一批。图节点只依赖 Feature 和窗口资格，不依赖未来标签是否存在。预测可以只返回请求证券，但同日其他合格证券仍参与图。

序列采样索引在交给官方DailyBatchSampler前统一为日期领先，实际取值同步映射到原存储位置。样本模型使用`sample`及正整数`batch_size`。两种批次都完整消费尾批，单证券日保留股票轴。

训练、预测、负MSE早停沿用Qlib算法。

序列运行使用Qlib0.9.7、Torch2.9.1。CUDA运行精确使用Torch2.9.1+cu130，并显式声明`GPU=0`，CPU为`GPU=-1`。请求的GPU不可用时失败，不回退CPU。

模型配置保存设备、CUDA和分配及保留显存峰值。设备和依赖变化后重新准入。

项目模型通过`research_pipeline.extensions.build_project_model_adapter(bundle_path, module, class_name, network_attribute="GAT_model", batch_contract="daily_cross_section")`绑定已有受控项目算子bundle。候选使用固定`ProjectQlibModel`及`research_pipeline.research.modeling.project_model`，`kwargs.adapter`保存builder返回的完整描述，其余kwargs交给冻结源码内的Qlib Model构造函数。项目模块和精确依赖均须属于该bundle，不能通过候选指定未封存源码路径。参数的项目语义由构造函数检查，核心不解释专用网络层数。

模型bundle保存完整项目bundle副本，包含manifest、依赖锁和源码。模型按源码身份建立独立模块命名空间。迁移后从封存副本加载，不需要原开发目录。恢复核对实际网络结构、显式权重、处理器、窗口和项目类型，输入或实现变化不能命中旧checkpoint。

正式序列允许learn标签处理器CSRankNorm。预测分数为`ranking_score`，原始收益仍为`actual`，模型误差使用`evaluation_label`。同一次候选搜索的标签口径必须一致。排名分数不能解释为预期收益，Feature处理器仍仅拟合合格train末端。

## 输入工作集与资源

split 与 locked holdout 默认声明 16 GiB。Feature 和 opened 后的 Label 完整记录逐批核验，来源和时间字段都参与原摘要。模型必需时间列投影后继续执行可见性门禁，未参与训练的来源字段篡改仍拒绝。

development 标签继续按留出边界隔离，locked holdout 必须先登记 opened 才加载目标。已消费留出不能重新训练。列投影不改变样本、缺失值、处理器、模型、指标或 PIT 规则。

加载工作集和小批测量见[资源预算](project_resource_budgets.md#日频模型输入与序列预算)。

## 正式表引用式模型事实

模型结果保存正式预测、误差、选择、模型文件与必要窗口表。正式引用关联研究设计、配置、输入版本和 holdout 账本。开发与最终研究共用生产合同，不为每轮研究生成额外验证事实副本。金融组合保留正式账本和指标。

序列工件使用 research.model-sequence-context.v2。context为证券会话分配context_index，members保存sample_id、step和context_index。特征、时间与lineage保存在共享context，模型子集保留原编号。

Qlib数据集直接按context_index选择历史上下文，只接受该紧凑成员结构。生产构建检查冻结日历、原始Feature/Label与完整窗口，开发用例按手算检查末端资格。

序列模型使用research.qlib-sequence-model-bundle.v2和research.qlib-sequence-input.v2，表格模型沿用research.qlib-model-bundle.v1。历史文件保持原样。输入或实现变化后重新准入，不跨身份恢复，不重置已消费的holdout。

## 最终评价的研究系列

`research.model.locked-holdout` 使用必填 `research_series_json` 冻结系列 ID、持久 Root、研究问题和旧账本根。具体设计身份继续用于本次变体，父研究目的由稳定系列确定。改窗口、候选、seed 或数据版本后，既往最终暴露仍参加闭区间重叠检查。

四阶段账本及 `series.json` 随 Result 封存。系列级文件锁覆盖打开前重读与 `opened` 写入，目标值在记录成功后读取。失败消费和已成功节点复用继续遵守原合同。配置、旧账本登记和恢复操作见[研究系列与最终留出集](holdout-series.md)。


## 候选与 fold 并行

正式 `run` 或 Workspace 执行使用 `--mode bounded_parallel --workers 2 --resource-memory-bytes 34359738368` 可请求候选与 fold 的并行拟合。32 GiB 总包络为已声明 16 GiB 的 split 节点和 Supervisor 驻留留下空间，实际容量按研究声明配置。`partitioned_batch` 使用分批调度，默认串行保持不变。

内建 fit 声明 2 个 CPU 槽和 10 个节点进程槽。额度覆盖 Windows 虚拟环境的外层计算进程。额度还覆盖 2 个候选进程和文件 Recorder 辅助进程。

候选的内部线程数继续由原模型合同限制。workers 不额外分配内存，跨进程治理还需为共享 Supervisor 留一个槽。冻结搜索清单的 max_parallel 等于声明候选数，串行与并行使用同一候选宇宙上限。

单个候选的多个 fold 可独立并行，不新增候选试验。实际任务并发继续受运行资源约束。

每个 spawn 子进程在本次 worker 会话首次读取时完整核验来源。核验包括封存 split 的元数据、samples 和序列窗口。后续读取复用已核验的只读内存输入和 fold 行索引。每项任务仍核对冻结 split 身份和元数据。来源或身份变化不能命中原输入，新的 worker 或恢复进程重新完整核验。

复用范围只到该 worker 会话结束，不跨进程保存。常驻样本、序列窗口、fold 索引及候选副本一并纳入节点内存预算。每个候选／fold 取得独立训练和 validation 副本。每个候选／fold 分别拟合处理器与模型。拟合使用原参数和根种子，并写入独立 bundle 路径。

Supervisor 按固定候选／fold 顺序汇总拟合记录、范围证书和 trial ledger。预测、选择、最终重训与 holdout 的原口径保持。并行拟合不打开 test/holdout。

父进程驻留、子进程输入副本和运行时开销共同限制实际并发。实际内存、temp 和 deadline 超限会停止并返回失败。失败状态不会作为金融指标，未完成 fit 不发布成功 checkpoint。执行模式和 workers 随原 invocation 保存，恢复须复验同一配置与实现身份。

拟合期间的目录大小按50ms周期采样，与现有资源监控周期一致。内存、进程数和deadline仍在任务等待循环检查。系统在启动前、每批任务完成和全部 worker 退出后强制统计目录。模型正式表归并前后继续检查完整输出大小。

执行摘要记录temp采样次数、周期和观测峰值。temp保持采样约束，不表示能捕获两次采样之间已经删除的瞬时文件。

## 有界向量Feature与显式日期fold

模型输入可使用原有标量Feature，也可把同一证券会话的一组特征保存为`value: list<float64>`。向量名称唯一承载位置是Arrow的value字段metadata：`model_feature_value_names`，内容为固定、非空、无重复的JSON字符串列表。所有文件的名称及顺序必须一致。

长度不符、字段metadata缺失或混合标量/向量分片会拒绝，不猜测列名。展开列使用`名称__w窗口会话数`。窗口会话数包含观察日。

向量是通用值表达，不将整组伪装成单个标量，也不先展开成巨量长表。

系统逐批核对完整来源记录，再按证券归组为宽上下文。临时副本位于节点工作范围内。成功转换后释放临时副本。数值NaN交给冻结的infer处理器。缺少证券会话、缺少声明列或特征不可见仍不具完整序列资格。

Label 允许保留当时合格股票的缺收益行。每日图资格使用候选索引和 Feature。未来收益是否有限不影响图节点资格。拟合及评价只消费有限真实标签，预测表保留完整候选。

split可声明`explicit_folds_json`，格式为`[{"fold_id":"fold1","train":["2006-01-01","2010-12-31"],"validation":["2011-01-01","2011-12-31"],"test":["2012-01-01","2012-12-31"]}]`。日期按观察会话确定，不以252会话近似自然年。训练及验证边界仍按真实标签结束和成熟时间purge。

selection可显式设置`emit_all_test=true`保存每个固定候选的开发test预测，限development范围。逐时点winner仍只由当时可见validation决定，不能据未来test回选历史模型，locked-holdout合同保持独立。

每日横截面候选可通过dataset.graph_layers正整数声明注意力层数的资源形状。它只用于预算估计，不修改模型网络。原生GAT固定1，项目模型准备阶段核对其与实际构造层数一致。

LSTM门数及每日N²×双端hidden工作集共同计入估计。单GPU拟合串行，实际allocated/reserved峰值随模型配置封存。

模型fit、验证predict和开发test选择阶段按冻结fold读取样本，窗口context仅载入当前members实际引用的行。不同时保留全历史samples与prediction_samples。每阶段完整核验原始工件记录身份后，后续fold复用核验结果并按列式条件读取。

同fold候选共用一份已绑定窗口，跨fold释放。模型恢复继续核对完整冻结 calendar。消费 context 的日期和证券决定 Qlib 实际索引规模。

系统不把全历史日历复制成特征稠密立方体。

Split阶段的输入列、逐fold审计缓冲、共享时间字符串及最终审计表共同计入节点内存预算。审计分配和排序前按实际行数检查工作集。跨fold共用的字符串对象只登记一次，临时列缓冲随释放退出预算。拟合样本与完整截面预测样本分别保存，缺失未来标签不改变预测节点资格。

日频样本先在窄元数据上验收唯一性、标签资格和可见时间。系统稳定排序后，按最终位置提取宽特征列。序列窗口先规范化证券与会话键并筛选冻结日历，再建立context索引。全量保留时仅创建独立列结构并共享只读特征值。输入表不被修改，输出行列顺序、dtype及缺失窗口排除口径保持。

Fit入口逐批完整复验正式split审计记录与冻结hash，随后只保留scope签发所需的fold成员和manifest身份。正式审计仍保存在split工件，不在所有候选训练期间常驻内存。

序列拟合准备只在窄证券会话键上选择和排序历史行，宽特征按最终位置提取一次。处理器使用独立输入并保持原字段选择。处理结束后释放拟合及标签临时矩阵，再组装Qlib特征、标签和endpoint filter。预测标签仍为NaN，模型及处理器恢复规则不变。

每个拟合任务重新读取冻结split元数据并核对当前内容。同worker会话验收后复用同一元数据对象，不在候选拟合期间保留重复解析副本。串行主入口与fold reader共用已验收引用。

输入变化仍拒绝。候选按整数行位置生成独立train/valid，不追加同内容副本。

内部候选拟合入口交接源样本及行位置，由fit函数创建并独占宽train/valid副本。调用方不在参数栈保留这些宽表。配置所需样本ID和时间先冻结，数据集构造完成后释放宽输入，再初始化模型并训练。预算检查按有界分段的实际列字节计算，保持原标签和拟合时间口径。

预测和选择按年度消费完整候选样本及其历史窗口，共用入口已读取的split元数据。来源表仍逐批完整验收。年度交接释放上一年样本和窗口的真实引用后再加载下一年，不只减少预算计数。

Qlib handler 接收数据后，表格模型释放拟合处理器和拼接临时矩阵。训练期间只保留 handler 实际消费的数据。模型与处理器的训练字段及保存内容保持不变。

已完成封存模型可通过[显式模型输入](completed-model-input.md)接入新研究，来源训练状态与本次执行分开保留。

Fold输入先按Parquet有界批次解码，再应用冻结候选或context索引过滤，保持文件与记录顺序。该路径避免年度读取中整行组筛选的原生工作区累积。空投影保留原schema，完整来源记录验收继续执行。


## 无成熟标签的预测资格

`research.model.split-manifest`可显式连接`prediction_index`，类型为`research.model-prediction-index.v1`，表名为`prediction_index`。行含`entity_id`、`observation_session`、`observation_time`、`decision_time`、`horizon_sessions`、`eligibility_available_time`、`lineage_hash`。时间带时区，资格和Feature必须在决策时点可用，同一证券、决策会话和horizon唯一。工件的`semantics_hash`须与Feature、Label相同。

预测资格由研究当时可见的资产池产生，不从完整Feature上下文推断。为未来候选保存的历史Feature只用于序列，不能提前成为当前截面节点。显式索引先决定预测键，再连接Feature和真实Label。

Label行缺失或收益为空均保留推理，不能填造未来价格。无完整Feature或完整序列窗口的末端沿现行规则排除。

训练和验证评分仍只使用成熟且有限的标签，跨界purge保持。预测按冻结 fold 的观察日起止选择资格。显式日期范围和滚动日历范围均进入 split 身份。系统不以有限标签的日期极值裁剪资格。测试无有限标签时预测仍发布，评分为空、样本数为零，不登记虚构的零分。预测表的actual、raw_label和evaluation_label为空值，未知标签时间为可空字符串字段中的null而不是`NaT`文本。

开发模式预测索引禁止触及locked holdout决策边界。最终模式将索引按真实决策时点拆成开发和locked两部分，locked资格仅冻结为独立表，不加入开发预测。完整窗口筛选只保留实际合格末端。

正式holdout先登记opened，再在加载回调内读取冻结资格、Feature及真实Label。缺失Label的预测仍保留，评分仅使用成熟finite标签，全部未成熟时指标为null且sample_count为0。显式索引不会代替holdout 访问授权，失败消费不能重置。

输入合同、实现或资格变化需重新准入，旧计划及checkpoint不跨身份复用。

## 错误处理

| 现象 | 处理方式 |
| --- | --- |
| 样本为空或窗口不足 | 检查日历、前置历史、标签成熟时间和 purge 排除记录。 |
| GPU 不可用 | 安装与声明相符的环境，或修改设备声明后重新准入。 |
| 留出区间已消费 | 沿用可复验的成功节点，或选择独立的未来区间。禁止重置账本。 |
