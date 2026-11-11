# 稳健研究与统计

## 开始前

- 明确研究问题、主估计量和比较基准。
- 固定候选全集、样本区间和随机协议。
- 准备已准入的特征和标签工件。
- 将统计方法及指标定义写入研究包。

稳健统计用于描述估计误差和选择偏差。
按时点可见（PIT）规则约束输入信息。
最终留出集（holdout）独立于开发选择。

## 设计与读取流程

1. 声明样本切分和标签重叠政策。
2. 声明预处理的拟合范围。
3. 固定统计方法、检验方向和校正规则。
4. 执行正式研究包。
5. 读取 Result 的指标和统计工件。
6. 按实际样本、退化状态和限制解释结论。

通用统计函数不会自动组成完整项目流程。
项目扩展负责具体因子、分组和事件研究算法。

`research_pipeline.research` 提供项目无关的分钟研究原语、样本外验证、搜索治理和稳健统计。它只消费数据平面工件，不连接数据库，也不发布正式因子库。完整因子研究方法由项目扩展维护。

## 统计名称

| 名称 | 用途 |
| --- | --- |
| HAC（异方差与自相关稳健误差） | 估计存在异方差和时间相关时的误差 |
| block bootstrap（分块自助法） | 按连续块重采样，保留块内依赖 |
| PSR（概率夏普比率） | 评价夏普比率超过基准的概率 |
| DSR（折减夏普比率） | 估计试验选择对夏普比率的影响 |
| PBO（回测过拟合概率） | 描述样本内选择在样本外失效的风险 |
| Reality Check（现实检验） | 对候选全集检验优于基准的证据 |
| SPA（优越预测能力检验） | 检验候选相对基准的预测能力 |
| purge（清除跨界标签） | 删除跨越样本边界的标签 |
| embargo（隔离区间） | 在样本边界保留隔离会话 |
| BY（Benjamini–Yekutieli 校正） | 控制依赖检验中的错误发现率 |

## 能力

- 统计：HAC、聚类稳健误差、重叠标签修正、block bootstrap、多重检验、PSR、DSR、PBO、Reality Check 和 SPA。
- 验证：purge/embargo K-Fold、walk-forward、nested validation、fit 审计、唯一候选或完整候选族 holdout，以及完整试验账本。

Bootstrap 的正式索引接口为 `iter_bootstrap_index_batches()`，selection-bias 的 Reality Check/SPA 消费批次，不创建 replication×样本数完整索引矩阵。固定 seed 的随机顺序不随批大小改变。模型使用六节点 Runtime，保留候选拟合和 holdout 内层状态机。结果结论沿用声明事实。

IC、分组收益和公告市场模型这类完整研究方法留在项目。ResearchPackage、Operator bundle 和 metric_definitions 冻结算法、表、单位及指标定义。开发验收用手算与确定性样本。项目分析遵守生产 PIT、标签隔离、最终留出与 Result 合同。

## 研究语义与项目分析

公共 `ResearchSemantics v1` 由 FeatureSetArtifact、LabelArtifact、EstimandSpec 和
HypothesisSpec 组成。Feature 每行包含决策时点和来源实际可见时间。Label 每行包含决策时点、第一条真实观测、最后一条真实观测与可见时间。

第一条观测必须严格晚于决策时点。项目扩展只返回键和值，时间事实由框架根据已准入数据附加。

IC、分组收益和公告市场模型这类完整研究方法不在公共 Operator 或 Metric 注册表。
项目在 ResearchPackage、Operator bundle 与 `metric_definitions` 中冻结算法、表结构和指标定义。实现验收使用确定性样本、手算和必要的差分测试。项目准入前不能把
历史参考样本的结论当成当前公共能力。项目分析遵守 PIT、标签隔离和正式 Result 合同。


## 失败关闭

系统拒绝标签泄漏、未来发布和最终修订冒充历史修订。系统也拒绝缺 PIT 暴露、test/holdout 选参和未冻结搜索空间。缺失试验和非法预处理同样导致拒绝。holdout 只有唯一候选确认和完整候选族确认两种模式。后者必须在打开前同时冻结候选全集、BY 这类校正规则、alpha、方向、主估计量和随机协议。

无值预检失败不消耗，`opened` 后失败仍消耗，完整候选族随后永久退役。未参与预声明选择规则的区间统一叫 `diagnostic`。容量指标只是研究代理，不是成交能力承诺。

真实统计输出使用版本化 `MetricDefinition`。定义绑定输入 Artifact 类型、输出 schema、单位和频率。定义还绑定年化、无风险利率、缺失值、方向和实现字节摘要。

稳健统计目录同时写出 `metric-artifacts.json`。每个 `MetricArtifact` 绑定定义摘要、样本范围和来源 Artifact lineage。工件还绑定 implementation digest，并保存 `metric_id`、`version` 与组合引用。没有真实产物的指标不能仅靠名称或声明冒充已执行指标。

项目使用多候选统计时，声明必须固定主估计量、候选全集和比较基准。声明还必须固定 HAC 或聚类政策、校正规则和随机协议。正式指标来自实际生产表。通用统计函数不会自动组成完整项目流程。

矩阵算法的开发用例应同时覆盖普通矩阵、宽矩阵、全零首行和线性依赖。普通矩阵按列满秩
设计。宽矩阵的目标秩为 `min(列数, 非零有效行数)`。实际统计结果保存样本范围、
退化状态及所用处理政策，不能把缺失结果填成正常指标。

正式因子库写入仍必须使用 `factor_calc.publish`，并在写库前取得用户确认。

## 分钟统计 profile

分钟研究使用 `MinuteStatisticsProfile v1`，输入是带 `decision_at_ns / entry_at_ns / exit_at_ns`、候选 ID、观察 ID 和收益的原始标签工件。框架按真实 entry/exit 重叠数推导 HAC lag 下限，并以最长标签跨度推导 embargo 下限。ResearchPackage 可以选择更保守的值，但不能降低下限。train、validation、test 边界在读取测试结果前锁定，跨边界标签和 embargo 区间只会被 purge，不会被挪到相邻集合。

trial universe须非空、唯一并稳定排序，固定时钟、候选、指标、方向、alpha和依赖假设共同形成清单身份。每个候选test集至少30个有效观察，研究包只能提高下限。失败和退化候选保留终态，不能删除后重新计算显著性。支持Holm与Benjamini–Yekutieli校正。

当前`research.statistics.minute` Runtime adapter只接受单候选、单标的有序标签来源。底层profile的数据结构不能当作主DAG已经支持多候选来源的证明。统计工件保存观察、split分配、推断、校正、profile与trial身份。

HAC或embargo低于重叠下限、样本不足或统计退化时拒绝或记录失败。结论上限为`historical_intraday_research_observation`，不表示实时可用、可交易或生产绩效。

## 错误处理

| 现象 | 处理方式 |
| --- | --- |
| 样本不足或统计退化 | 保存失败或退化状态，检查声明与实际样本。 |
| HAC 或 embargo 低于下限 | 按真实标签重叠提高声明值。 |
| 试验记录缺失 | 恢复完整候选全集及终态记录。 |
