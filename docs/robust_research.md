# 稳健研究与统计

`research_pipeline.research` 提供项目无关的分钟研究原语、样本外验证、搜索治理和稳健统计。它只消费数据平面工件，不连接数据库，也不发布正式因子库。完整因子研究方法由项目扩展维护。

## 能力

- 统计：HAC、聚类稳健误差、重叠标签修正、block bootstrap、多重检验、PSR、DSR、PBO、Reality Check 和 SPA。
- 验证：purge/embargo K-Fold、walk-forward、nested validation、fit 审计、唯一候选或完整候选族 holdout，以及完整试验账本。

Bootstrap 的正式索引接口为 `iter_bootstrap_index_batches()`，selection-bias 的 Reality Check/SPA 消费批次，不创建 replication×样本数完整索引矩阵；固定 seed 的随机顺序不随批大小改变。模型使用六节点 Runtime，保留候选拟合和 holdout 内层状态机。结果结论沿用声明事实。

IC、分组收益、公告市场模型等完整研究方法留在项目。ResearchPackage、Operator bundle 和 metric_definitions 冻结算法、表、单位及指标定义；开发验收用手算与确定性样本。项目分析遵守生产 PIT、标签隔离、最终留出与 Result 合同。

## 研究语义与项目分析

公共 `ResearchSemantics v1` 由 FeatureSetArtifact、LabelArtifact、EstimandSpec 和
HypothesisSpec 组成。Feature 每行包含决策时点和来源实际可见时间；Label 每行
包含决策时点、第一条真实观测、最后一条真实观测与可见时间，且第一条观测必须
严格晚于决策时点。项目扩展只返回键和值，时间事实由框架根据已准入数据附加。

IC、分组收益、公告市场模型等完整研究方法不在公共 Operator 或 Metric 注册表。
项目在 ResearchPackage、Operator bundle 与 `metric_definitions` 中冻结算法、
表结构和指标定义，用确定性样本、手算与必要的差分测试验收实现。项目准入前不能把
历史参考样本的结论当成当前公共能力；项目分析遵守 PIT、标签隔离和正式 Result 合同。


## 失败关闭

标签泄漏、未来发布、最终修订冒充历史修订、缺 PIT 暴露、test/holdout 选参、未冻结搜索空间、缺失试验和非法预处理都会被拒绝。holdout 只有唯一候选确认和完整候选族确认两种模式；后者必须在打开前同时冻结候选全集、BY 等校正规则、alpha、方向、主估计量和随机协议。无值预检失败不消耗，`opened` 后失败仍消耗，完整候选族随后永久退役。未参与预声明选择规则的区间统一叫 `diagnostic`。容量指标只是研究代理，不是成交能力承诺。

当前真实统计输出使用版本化 `MetricDefinition`：定义绑定输入 Artifact 类型、输出
schema、单位、频率、年化、无风险利率、缺失值、方向和实现字节摘要。稳健统计目录
同时写出 `metric-artifacts.json`，每个 `MetricArtifact` 绑定定义摘要、样本范围、
来源 Artifact lineage 和 implementation digest，并显式保存 `metric_id`、`version`
与组合引用。没有真实产物的指标不能仅靠名称或声明冒充已执行指标。

多候选稳健统计算子的主估计量是候选净收益减自定义基准净收益的时间序列均值，使用
Bartlett Newey-West HAC；交易日数量不是聚类簇数，结果会如实记录
`cluster_dimensions=0`、实际 HAC lag、lag policy、置信区间方法和 BY 校正。SPA 的
stationary bootstrap 真实使用 package 声明的 `bootstrap_seed`、重复次数和块长；
DSR/PBO/SPA 是当前算子版本固定执行的方法集合。未使用的 `risk_free_rate`、
`coverage_threshold` 和虚假的方法选择参数已经移除，输入完整性继续由硬门禁保证。

矩阵算法的开发用例应同时覆盖普通矩阵、宽矩阵、全零首行和线性依赖。普通矩阵按列满秩
设计；宽矩阵的目标秩为 `min(列数, 非零有效行数)`。实际统计结果保存样本范围、
退化状态及所用处理政策，不能把缺失结果填成正常指标。

正式因子库写入仍必须使用 `factor_calc.publish`，并在写库前取得用户确认。

## 分钟统计 profile

分钟研究使用 `MinuteStatisticsProfile v1`，输入是带 `decision_at_ns / entry_at_ns / exit_at_ns`、候选 ID、观察 ID 和收益的原始标签工件。框架按真实 entry/exit 重叠数推导 HAC lag 下限，并以最长标签跨度推导 embargo 下限；ResearchPackage 可以选择更保守的值，但不能降低下限。train、validation、test 边界在读取测试结果前锁定，跨边界标签和 embargo 区间只会被 purge，不会被挪到相邻集合。

trial universe 至少包含两个稳定候选，固定时钟、候选集合、指标、方向、alpha 和依赖假设共同形成清单摘要。每个候选的 test 集硬下限为 30 个有效观察，package 只能提高，不能降低；每个候选都必须保留一条终态结果，包括样本不足或统计退化的候选。首版分钟 profile 支持 Holm 和在任意依赖假设下更保守的 Benjamini–Yekutieli，不允许遗漏失败候选后重算显著性。

分钟统计 artifact 还绑定 Catalog、data snapshot、QueryIR、session、OperatorDefinition、分钟质量、仿真、Runtime manifest、恢复一致性和 trial universe 十类摘要。质量失败、串行/恢复摘要不同、仿真存在 unsupported、样本不足、HAC/embargo 低于下限或多重检验不闭合时，artifact 为 fail。其分钟专用结论上限固定为 `historical_intraday_research_observation`；通用 Evidence 等级仍映射为更保守的 `research_observation`，不宣称因果、实时可用、可交易或生产绩效。
