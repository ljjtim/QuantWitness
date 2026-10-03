# Qlib 日频模型与样本外选择

模型计算采用 Qlib 0.9.7 的 Dataset、Processor 与 Model，首批支持日频、单 horizon、单标签回归：LinearModel 的 OLS/Ridge、LGBModel、XGBModel。安装使用 `pip install -e ".[ml]"`。分类、序列、多标签和在线更新不在当前支持范围。

## 六节点主链

`split-manifest → fit → predict → fold-metrics → selection → locked-holdout`

split 只组织准入样本和角色。fit 在每个 candidate/fold 内依次拟合 Qlib Processor 和 Model，删除独立 preprocess-fit。predict 应用冻结处理器，不再次 fit。validation 用于早停和候选选择，属于开发区；历史 test 候选只使用当时已可见的开发区指标。最终 holdout 在配置冻结后评估，不进入调参或 AI 反馈。

原始日线可通过[受控项目扩展](../project_extensions/README.md#中国股票日频来源)声明 `next_session_open` 时间绑定，使用冻结交易日历生成核心时间事实。模型 Label 时间列必须是带时区的时间戳；扫描边界按列时区表达同一时刻。

真实标签起止与 available_time 参与 purge、拟合和选择。split 先检查 Label row group，读取开发目标而不扫描尚未打开的 holdout 标签。最终 holdout 仍使用 prepared/opened/terminal 一次性账本，打开后的失败不会恢复访问资格。训练范围、验证角色和最终重训声明都进入证据。

## 候选声明

candidate_jsons 是完整 JSON 字符串列表，每项包含 candidate_id、model、processors、fit。model 使用实际支持的 Qlib class、module_path、kwargs；processors 使用 infer/learn 列表；fit 是该模型的训练参数。首批 CPU 单线程，有随机性的模型显式 seed。训练与最终重训节点各声明4个进程槽，覆盖 Qlib 记录器及依赖的辅助进程；线程数与进程槽分别约束。不提供旧 sklearn 后端、median_* 预处理、相关性 top-k 或 simple_model_gate_passed 开关。

```json
{"candidate_id":"ridge","model":{"class":"LinearModel","module_path":"qlib.contrib.model.linear","kwargs":{"estimator":"ridge","alpha":0.1,"fit_intercept":true,"include_valid":false}},"processors":{"infer":[{"class":"RobustZScoreNorm","kwargs":{"fields_group":"feature","clip_outlier":true}},{"class":"Fillna","kwargs":{"fields_group":"feature","fill_value":0}}],"learn":[{"class":"DropnaLabel","kwargs":{}}]},"fit":{}}
```

Linear 使用训练期拟合的稳健标准化，再填充 feature 缺失。LGB/XGB 保留数值与 NaN，不强制缩放。首批候选默认使用原始收益标签；若另行采用 CSRankNorm(label)，分数为排名分数，原始 actual 仍独立保存供收益评价。不能以排名分数充当预期收益率。同一搜索的候选必须采用相同的标签口径；选模指标使用 `evaluation_label`，原始收益仍保留为 `actual`。

训练窗配置不能替代实际合格样本选择：purge/不可见记录先剔除，再交 Qlib。infer 不因未来标签未知而删除行。每交易日每证券每 horizon 一行，Qlib datetime 对应 observation_session，instrument 对应 entity_id，真实带时区的可见时间另行保留。

## 模型文件与恢复

使用现有外部工件存储；models 表保存相对 bundle 路径，bundle 包含 model.pkl、已拟合 Processor 文件与 config.json。不保存 Dataset、Handler 或训练矩阵。树模型的 train/valid 训练曲线保存于配置和 `learning_curves` 表，Linear 的曲线表为空。FitScope 证明处理器使用的实际训练样本范围，模型可用时点另包含 validation 标签成熟时间。模型和 Processor 使用 Qlib Serializable，加载推理仅应用状态。

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
