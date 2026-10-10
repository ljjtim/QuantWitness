# 模型研究：滚动训练与样本外评价

模型应该用过去训练，用之后的数据评价。滚动研究会沿时间推进训练和评价窗口；开发阶段用于比较候选，最终留出样本用于评价已经选定的方案。

这页说明 Qlib 模型、训练处理器、样本划分、候选选择与最终评价怎样接入同一研究流程。

## 参数与行为说明

模型计算采用 Qlib 0.9.7 的 Dataset、Processor 与 Model，支持日频、每模型单期限、单标签回归：LinearModel 的 OLS/Ridge、LGBModel、XGBModel、DEnsembleModel（样本重加权版本）以及 GRU/LSTM/Transformer 序列模型。安装使用 `pip install -e ".[ml]"`。分类、多标签和在线更新不在当前支持范围。

## 六节点主链

`split-manifest → fit → predict → fold-metrics → selection → locked-holdout`

split 只组织准入样本和角色。fit 在每个 candidate/fold 内依次拟合 Qlib Processor 和 Model，删除独立 preprocess-fit。predict 应用冻结处理器，不再次 fit。validation 用于早停和候选选择，属于开发区；历史 test 候选只使用当时已可见的开发区指标。最终 holdout 在配置冻结后评估，不进入调参或 AI 反馈。

原始日线可通过[受控项目扩展](../project_extensions/README.md#中国股票日频来源)声明 `next_session_open` 时间绑定，使用冻结交易日历生成核心时间事实。模型 Label 时间列必须是带时区的时间戳；扫描边界按列时区表达同一时刻。

真实标签起止与 available_time 参与 purge、拟合和选择。split 先检查 Label row group，读取开发目标而不扫描尚未打开的 holdout 标签。最终 holdout 仍使用 prepared/opened/terminal 一次性账本，打开后的失败不会恢复访问资格。训练范围、验证角色和最终重训声明都进入证据。

## 滚动窗口与多个预测期限

开发切分支持 expanding 和 rolling。前者保持训练起点，随时间增加历史；后者按 `step_sessions` 前移固定名义训练窗。切分日历按开发样本的 `observation_time` 日期裁切，窗口成员也按真实观察时间分组；`observation_session` 保留模型证券日期索引。前一会话收盘因子可以在下一决策会话使用，两种日期不要求相同。独立复核按相同观察时间重建成员，purge 继续使用实际决策时点、标签结束与成熟时间。每个候选在每个 fold 单独拟合处理器与模型。实际训练成员还要经过标签结束时间、成熟时间、embargo 和完整序列资格筛选；名义窗口不保证固定样本数。序列 warmup 可读取窗前已可见的 Feature，不把窗外标签加入拟合。

公开 Qlib 示例用 `--window-mode rolling --train-sessions 20` 声明滚动开发，用 `--horizon-sessions 5` 声明单个五会话目标，也可用 `--horizons 1 5` 一次冻结多份研究。每个期限独立生成 ResearchPackage、Result、VerificationResult 和最终留出账本，共享输入来源、候选、窗口配置及留出边界。组清单只记录各成员结果位置，不合并不同期限的 MSE，不用最终留出表现选择期限。

标签为 `close[t+h] / close[t] - 1`，在 t+h 会话收盘结束，下一会话开盘成熟。不同期限按真实成熟时间分别 purge；冻结设计、开发样本及最终预测的期限必须一致。开发模式只封存留出边界前的输入，不开放最终留出。

滚动选项控制开发 fold。最终赢家仍以全部合格开发历史重训，末尾 validation 窗口用于早停，训练成员继续按实际标签时间 purge。最终重训不缩为最后一个固定训练窗。准备、恢复与候选声明见[公开示例](../examples/qlib_portfolio/README.md#滚动与多期限研究)。

## 候选声明

candidate_jsons 是完整 JSON 字符串列表，每项包含 candidate_id、model、processors、fit。model 使用实际支持的 Qlib class、module_path、kwargs；processors 使用 infer/learn 列表；fit 是该模型的训练参数。首批 CPU 单线程，有随机性的模型显式 seed。训练与最终重训节点各声明4个进程槽，覆盖 Qlib 记录器及依赖的辅助进程；线程数与进程槽分别约束。不提供旧 sklearn 后端、median_* 预处理、相关性 top-k 或 simple_model_gate_passed 开关。

```json
{"candidate_id":"ridge","model":{"class":"LinearModel","module_path":"qlib.contrib.model.linear","kwargs":{"estimator":"ridge","alpha":0.1,"fit_intercept":true,"include_valid":false}},"processors":{"infer":[{"class":"RobustZScoreNorm","kwargs":{"fields_group":"feature","clip_outlier":true}},{"class":"Fillna","kwargs":{"fields_group":"feature","fill_value":0}}],"learn":[{"class":"DropnaLabel","kwargs":{}}]},"fit":{}}
```

Linear 使用训练期拟合的稳健标准化，再填充 feature 缺失。可另选训练期拟合的 ZScoreNorm，或同一会话横截面的 CSZScoreNorm；二者只处理 infer feature，不改变训练样本资格和标签口径。LGB/XGB 保留数值与 NaN，不强制缩放。首批候选默认使用原始收益标签；若另行采用 CSRankNorm(label)，分数为排名分数，原始 actual 仍独立保存供收益评价。不能以排名分数充当预期收益率。同一搜索的候选必须采用相同的标签口径；选模指标使用 `evaluation_label`，原始收益仍保留为 `actual`。

训练窗配置不能替代实际合格样本选择：purge/不可见记录先剔除，再交 Qlib。infer 不因未来标签未知而删除行。每交易日每证券每 horizon 一行，Qlib datetime 对应 observation_session，instrument 对应 entity_id，真实带时区的可见时间另行保留。

## 模型文件与恢复

使用现有外部工件存储；models 表保存相对 bundle 路径，bundle 包含 model.pkl、已拟合 Processor 文件与 config.json。不保存 Dataset、Handler 或训练矩阵。LGB/XGB 的 train/valid 训练曲线保存于配置和 `learning_curves` 表，Linear 与 DEnsemble 的曲线表为空；固定上游 DEnsemble 未导出曲线。DEnsemble 配置另封存子模型数量、特征顺序、权重和实际轮数，恢复预测核对实际模型状态。FitScope 证明处理器使用的实际训练样本范围，模型可用时点另包含 validation 标签成熟时间。模型和 Processor 使用 Qlib Serializable，加载推理仅应用状态。

要交付模型的 ResultSpec 显式选择 research.qlib-model-inventory.v1 表，ResultAssembler 收集其模型、处理器和配置为 support_files。读取使用 artifact_key/source_path，不能依赖原 run-root 或同名文件的第一个匹配。完整性验证只核对字节，不执行 pickle；模型加载仅面向可信本地工件。

新的模型阶段使用 v2 输入输出合同。旧模型 JSON 和旧七阶段 checkpoint 不被新实现接续，不自动迁移历史结果。历史 Result 保持原样；新方法的研究要新建运行，不能暗中以新结果覆盖历史。

## 预测和报告

预测包括 candidate/fold/stage/sample_id、证券、交易日、观察/决策/可见时点、预测值和原始 actual。预测按索引映射，不依赖数组行号。未知或未成熟标签不进入训练；学习标签变换不覆盖原始经济标签。

HTML 报告用请求显式选择表、候选、阶段、fold/horizon 和日期范围，生成 Qlib 分组、IC/Rank IC、自相关。重复证券日期不自动 keep-last。算术累计收益明确标示；模型诊断没有交易账本，不生成虚构的策略净值或成本结论。无定义 IC 或不足分组样本显示缺失说明，不填零。

## 模型有效性与交付

包含训练和候选选择的研究需要模型有效性事实，不能使用把标签与搜索声明为不适用的数据行数观察算子。[日频模型预测有效性扩展](../project_extensions/README.md#日频模型预测有效性)采集正式输入，框架独立复核开发区切分、训练范围、validation选择、逐时点test选择、一次性holdout及MSE。事实逐行绑定Result表，模型配置和holdout四阶段账本随Result封存；验证不反序列化模型或重新训练。

纯预测诊断的金融门禁为`not_applicable`。接入日频现金仿真的组合研究声明金融适用，并绑定正式Result中的六表、TCA和独立金融oracle；复核通过后`financial.tradability`为`pass`，不能因存在模型诊断而跳过金融验证。

最终holdout成功提交后，即使下游节点失败，其访问资格仍已消费。按[严格失败节点复用](runtime.md)恢复时，所有指定节点及上游须在Worker启动前通过原身份与完整性检查；无法复用必须停止，不能回退为再次拟合或打开holdout。

## 能力范围

能力保持 local_only。已完成固定32证券的真实日频小样本验收：Ridge、LGB、XGB三个模型、三个walk-forward窗口、9组直接Qlib开发区对照，以及10个封存模型的13组预测恢复；对照与恢复最大绝对差均为0。最终holdout为704条预测、一次打开和一次提交，正式Result、独立验证及HTML报告通过。

该案例为未复权价格变化诊断，不证明全市场预测收益、策略盈利或独立清洁安装。确定性合成测试继续覆盖时间隔离等边界。未知程序错误不能被当作候选淘汰后静默继续。本地验收的行情与结果不随公开源码分发。


## 公开开发研究

[合成ETF起点](../examples/qlib_portfolio/README.md)提供开发区、最终模型和完整组合三种声明。开发区只使用训练与validation结果，研究记录和最终holdout分开；进入参数循环时，输入归档也必须限制在批准的开发截止内。

LightGBM训练记录保留节点内指标和train/valid曲线，不采集整仓Git差异；来源以RP已封存工件为准。训练记录启动或结束失败时恢复原Qlib和MLflow状态，原始训练错误保留供诊断。

## Double Ensemble 样本重加权

`qlib.contrib.model.double_ensemble.DEnsembleModel` 使用多个 LightGBM 子模型，按先前子模型的训练损失调整后继样本权重。沿用相同的 DatasetH、训练期处理器、validation 选模、test 预测和最终 holdout 重训入口。

```json
{"candidate_id":"double_ensemble","model":{"class":"DEnsembleModel","module_path":"qlib.contrib.model.double_ensemble","kwargs":{"base_model":"gbm","loss":"mse","num_models":2,"enable_sr":true,"enable_fs":false,"decay":0.9,"bins_sr":10,"epochs":100,"early_stopping_rounds":20}},"processors":{"infer":[],"learn":[{"class":"DropnaLabel","kwargs":{}}]},"fit":{}}
```

要求至少两个子模型、有限正数 decay、显式关闭特征选择。轮数和早停放在构造参数，fit 留空。CPU 单线程和根种子由 RP 注入。可声明 sub_weights，数量须匹配子模型，非负且首项为正；模型内部每个加权前缀都必须有正权重。独立验证核对声明、训练范围与封存状态，不反序列化模型。

当前新增验收使用合成数据，覆盖直接 Qlib 对照、保存恢复、未来标签隔离、开发区拟合、validation 预测、逐时点 test 选择和一次性 holdout 重训。特征选择、序列模型及真实数据 Double Ensemble 案例另行验收。


## 序列模型与完整窗口

`research.modeling.sequence` 提供完整窗口，`research.modeling.qlib.fit_bundle/predict_bundle` 已支持显式 `sequence_context` 的 GRU/LSTM/Transformer TS 模型文件接口。固定模块为 `qlib.contrib.model.pytorch_gru_ts.GRU`、`qlib.contrib.model.pytorch_lstm_ts.LSTM` 与 `qlib.contrib.model.pytorch_transformer_ts.TransformerModel`，数据集为 `TSDatasetH`。安装使用 `pip install -e ".[ml-sequence]"`，固定 Qlib 0.9.7、Torch 2.5.1；GRU已完成 Windows CPU 正式研究包和 Linux CPU 模型/安装专项验收。正式 ResearchPackage 支持显式序列声明的 GRU、LSTM、TransformerModel 与表格混合候选。split 必须提供 `sequence_step_len>=2`，fit 和 holdout 的序列窗口长度须一致，并绑定同一对应 split；每个序列模型节点至少声明一个序列候选。缺少或不一致的窗口在编译时拒绝。

`build_sequence_windows` 从原始 Feature 长表和显式末端构建窗口。末端只含 sample_id、证券、观察会话和 decision_time；历史上下文不依赖 Label，不携带 target。窗口按显式冻结交易日历取同证券的连续会话，包含当前观察会话，严格保持声明的特征顺序。原有表格模型与序列窗口共用日频特征整理函数，观察时间和可见时间均不得缺失。

返回的 `SequenceWindows` 保存 context、targets、members、exclusions、特征顺序、日历和 step_len。每个窗口成员记录步序、证券、会话、观察时间、可见时间和既有特征来源引用。上市历史不足记录 `insufficient_history`，缺会话或缺少一列有效 Feature 记录 `missing_feature`，成员在末端 decision_time 后才可见记录 `feature_not_visible`。标签 purge 不删除仍可使用的历史特征；validation 和 holdout 的首个末端可以使用此前已知的历史行。

`qlib_sequence_dataset` 接收上述窗口和明确的末端分段，返回 Qlib `TSDatasetH`。训练只接受不重叠且严格先后的 train/valid，显式标签恰好覆盖这些末端；test 不接收标签，末列为 NaN 占位。没有标签的历史行保留在上下文中，由过滤列限定哪些行成为样本末端。显式 NaN 特征可以由调用方使用冻结处理器转换；转换结果必须保持上下文索引、特征顺序，实际使用的完整窗口须全部有限。缺会话不能由填充处理器补造。

`normalize_candidates` 对 GRU/LSTM/Transformer 固定 `loss=mse`、`metric` 为空或 `loss`、CPU、`n_jobs=0`、`batch_size=1`、`step_len>=2` 和 `complete_window`。d_feat 可省略，由每个时点的特征数注入；显式给出时必须相符。root_seed 注入模型，训练与预测期间使用 CPU 单线程，结束或异常时恢复线程数及 NumPy/Torch 随机状态。

GRU/LSTM/Transformer 的训练期标准化和 Fillna 仅在合格 train 末端拟合，再应用到实际窗口上下文。当前不支持 CSZScoreNorm 特征处理器，因为它需要另行声明每个末端决策时点的可见证券集合。底层模型文件接口可使用标签 CSRankNorm，原始收益保持不变；正式序列研究的MSE评价要求raw标签，公开准入拒绝CSRankNorm。模型文件封存完整 Qlib model、独立 weights.pt、处理器、negative_mse 学习曲线和实际使用的无标签历史上下文、末端及成员事实。路径使用 bundle 内相对位置，迁移目录后可继续预测。

单样本输出形状由实例级 forward hook 保持为一维，Qlib 的训练、早停和预测算法仍使用上游实现；源码副本保持原样。模型恢复核对配置、网络结构、显式权重与 model 文件中的权重，以及批次形状合同。预测只使用冻结 infer 处理器，标签末列全部 NaN，不需要传入真实 target。

`evaluate_locked_holdout` 的底层接口可提供 `development_sequence_context` 和 `holdout_sequence_loader`。最终开发模型先完成拟合，账本 opened 后才调用 holdout 数据与窗口加载器。打开后的加载失败会消费访问资格，再次调用不会重训或重新读取。

`evidence.sequence_validity.verify_sequence_window_facts` 从原始 Feature、冻结日历和末端独立重建上下文及窗口，核对数值、来源、时点、成员和排除理由，不调用生产构建器，也不加载模型 pickle。独立模型验证调用此函数复核序列事实；公开示例项目Verifier另从封存行情复算特征、标签、样本资格、切分和预测误差。

Runtime已接通split、fit、validation、metrics、selection和holdout的序列工件。切分接口显式提供`sequence_step_len>=2`，在Feature与Label拼接前确定全部末端的完整窗口资格；所有GRU/LSTM/Transformer候选须使用同一窗口长度，混合表格候选沿用筛选后的相同样本。缺失与晚可见末端记录到exclusions，历史Feature保留为预热上下文。工件保存context、targets、members、exclusions、日历及窗口声明，并纳入既有工件身份和节点内存预算。

最终holdout在opened之后重新读取已绑定的Feature/Label，按冻结合格末端加载标签，再构造实际预测窗口；最终开发模型仍先完成训练。开发研究的输入归档截止holdout前，图中没有selection和holdout节点，禁止把最终留出评价用于开发。

Result组件已支持序列权重与窗口文件封存，独立模型验证已接入研究级窗口重建、实际末端与原始Label绑定、模型训练窗口精确子集及GRU/LSTM/Transformer配置核验。开发区和最终留出区的真实模型工件均已验收，删除测试运行目录后可从Result恢复模型预测。

公开示例通过 `--sequence-step-len` 生成开发或完整模型研究，ResultSpec包含原始Feature、Label和研究级四张序列表。示例及命令见[Qlib研究起点](../examples/qlib_portfolio/README.md)。Windows已验证GRU最终赢家的正式留出、两次真实进程中断后的恢复和holdout单次消费；Linux已验证当前源码训练、Result离线恢复及1.2.0 wheel安装态模型。Linux正式ResearchPackage全流程仍待最终跨平台验收。LSTM已完成候选准入、真实训练、无标签预测、正式ResearchPackage最终重训和200条holdout预测，Result独立验证pass；已通过model_fit及model_holdout成功后的两次进程中断恢复，10节点复用且holdout单次消费；Linux当前源码与1.2.0 wheel安装态真实LSTM专项各7项通过。


### Transformer注意力模型

候选使用`TransformerModel`及固定TS模块。`d_model`为不小于2的偶数，`nhead`为正整数且整除`d_model`；`reg`为有限非负数。窗口长度为2至1000，与上游位置编码容量一致。其余训练参数遵守统一序列合同；`hidden_size`不适用于该模型。

上游使用完整历史窗口内的自注意力，预测末端只读取决策时点已可见的窗口成员；窗口内的位置互相注意不引入决策时点之后的数据。模型的特征投影、位置编码、注意力头、编码器层、前馈层和优化器状态在加载时核对。内存预算包含参数副本、固定位置编码和随窗口长度平方增长的注意力工作区。Transformer训练、早停与预测沿用Qlib，单样本输出保持一维；学习曲线继续记录negative_mse。

Transformer已完成Windows正式ResearchPackage及两次模型成功节点后的中断恢复：最终200条holdout预测，10节点复用，holdout单次消费，独立验证pass。Linux当前源码及1.2.0 wheel安装态同7项真实模型专项分别通过。Linux正式包全流程仍待最终跨平台交付验收。
