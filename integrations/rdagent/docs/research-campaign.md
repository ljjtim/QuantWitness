# 有界开发区研究

`campaign-run`让RD-Agent根据已完成的开发区评价提出下一假设，在冻结候选与预算内继续研究。支持正式ResearchPackage参数研究和已有预测校准。校准类型为已封存模型预测的向零收缩：`adjusted_prediction = shrinkage * prediction`，其中系数在0至1之间。标签不变，每个fold先计算均方误差，再对fold等权平均。候选必须有相同的样本键、证券、日期和标签。

这是开发区的校准分析。原预测来自已验证Result；新参数的开发误差记录在研究过程收据，不是新ResearchPackage Result，也不继承原模型的holdout成绩。校准在声明研究时点形成，不能把事后选出的系数用于宣称过去的实时预测能力。

## 预测校准入口

[两轮合成示例](../examples/prediction_campaign/README.md)无需数据库或模型账户。正式来源模式还需在RD环境安装QuantWitness核心包，以复核Result及VerificationResult。

```bash
python -m quantwitness_rdagent campaign-run --request "$CAMPAIGN_REQUEST"
python -m quantwitness_rdagent campaign-inspect --request "$CAMPAIGN_REQUEST"
python -m quantwitness_rdagent campaign-resume --request "$CAMPAIGN_REQUEST"
```

请求所有字段显式给出；以下路径由使用者替换：

```json
{
  "campaign_id": "prediction_calibration",
  "session_root": "/research/calibration-session",
  "source": {
    "kind": "verified_result",
    "verification_result": "/research/verification.json",
    "result_store": "/research/results",
    "result_id": "替换为正式Result标识",
    "table_id": "validation_predictions",
    "design_table_id": "study_design"
  },
  "development": {
    "start": "2025-01-06", "end": "2025-01-17",
    "as_of": "2025-01-24T15:00:00+08:00",
    "fold_ids": ["walk_forward_001"], "horizon_sessions": 1
  },
  "candidates": [
    {"id": "baseline", "model_candidate_id": "替换为原模型候选ID", "shrinkage": 1.0},
    {"id": "half", "model_candidate_id": "替换为原模型候选ID", "shrinkage": 0.5}
  ],
  "baseline_id": "baseline",
  "budget": {"rounds": 2, "evaluations": 2, "model_calls": 0, "output_tokens": 0,
    "max_output_tokens_per_call": 0, "max_rows": 10000, "memory_bytes": 67108864},
  "stop": {"target_mse": null, "min_improvement": 0, "patience": 2},
  "proposer": {"mode": "fixed_policy"}
}
```

正式来源固定消费`validation_predictions`和包含单行`design_json`的`study_design`。预测字段沿用Qlib主链，必须有candidate_id、fold_id、sample_id、entity_id、observation_session、prediction、actual、feature_available_time、decision_time、label_start_time、label_end_time、label_available_time、stage、horizon_sessions、score_semantics。实际stage全部为validation，score_semantics必须为raw_return_prediction。日期与fold由请求筛选；行数和内存预算不足时直接拒绝，不静默抽样。

标签可见时间不得晚于as_of；as_of必须早于封存研究设计的holdout_start。特征可见时间、决策、标签起止与可见时间必须按序。来源Result和VerificationResult须绑定且验证通过。正式完整性检查可能读取其他证明材料，但提案、评价和选优只消费开发预测；最终指标、test/holdout表、原最终选模和模型文件不进入提案提示。

## 假设与反馈

首轮固定评价baseline。后续提案只能从菜单中选择未尝试候选，声明已成功评价的父候选和理由，或主动停止。失败、越界、重复及不可计算候选保留在逐轮记录。成功候选按开发MSE选择；同值保留较早候选。

固定反馈策略使用已经完成的开发反馈：改善时优先检验较强收缩；未改善时切换剩余候选方向。它用于可重复的零费用运行，不是新模型生成。live模式调用已有.env代理客户端，提案提示只含允许候选、目标、基准和程序生成的开发评价。模型理由保留供人阅读，不回流为已验证事实。

需要实时模型提议时，将proposer改为以下结构，并给出获准的调用与输出额度：

```json
{"mode": "live", "model": "使用者选定模型", "base_url": "https://provider.example/v1"}
```

```bash
python -m quantwitness_rdagent campaign-run --request "$CAMPAIGN_REQUEST" --model-env-file "$MODEL_ENV_FILE"
```

.env字段与[集成说明](../README.md)一致，凭据不写入环境变量、请求或快照。模型调用前持久预约；已完成响应可复用，已预约或失败的调用不自动再次付费。固定模式模型预算必须全部为0，且不接收.env参数。

## 停止、恢复与输出

跨轮共同使用轮数、实际评价数、模型调用数及输出token预约额度。无效提案消耗轮数和已发生的模型调用，不消耗实际评价额度。到达目标误差、轮数或评价预算、模型预算、候选用尽、连续无改进，或提案器主动停止时结束。

`session_root`保存`request.json`、`development.json`、`rounds/`、`model-calls/`、`rd-logs/`及`outcome.json`。请求与实际开发输入冻结，恢复不得增加预算或改变范围。RD成功阶段快照负责恢复调度，已完成提案与评价收据负责避免重复工作。纯本地评价中断可重新完成原预约；模型调用中断不自动重付费。

outcome包含来源引用、每轮状态、父候选、开发误差、选择和停止原因。`holdout_evaluated=false`与`new_formal_result=false`明确评价范围。原正式Result保持只读。研究收据适合审阅下一步候选，不作为新模型样本外有效性的证明。

## 预测校准的适用范围

- 预测校准模式只改变封存候选模型及预声明收缩系数；需要重新训练模型时使用下文的ResearchPackage模式。
- 开发区多轮选择会使用同一份开发数据；其最优值是探索结论，新的最终检验须使用尚未消费且预先冻结的留出数据。
- 合成来源有明确fixture标记，只用于教学和自动化验收；不能代替正式Result来源。
- 实时模型提案和固定反馈策略共享状态与评价机制，但各自的实调用验收范围应单独说明。


## 执行新的研究包

`research_kind=package` 使用同一组 `campaign-run/resume/inspect` 命令。每个候选覆盖基包中明确允许的参数，通过现有包变体工具生成完整 ResearchPackage，再调用 Workspace 完成准入、执行、独立验证和报告。固定策略用于零费用复现；live提案仍使用显式 `.env` 与跨轮预约预算。

来源声明包含基包、归档清单、Catalog Lock、扩展和独立Verifier。目标声明表ID、schema、数值列、日期列、可见时点列、等值筛选、聚合方式和优化方向。先确认独立验证为pass，再投影获准指标；提案器不会收到整份报告、数据行或异常堆栈。

`source.verification_memory_bytes`可选声明独立验证内存上限，单位为字节，必须为正整数；提供时传给正式`verify --verification-memory-bytes`，省略时沿用CLI默认值。它与指标读取的`budget.memory_bytes`、执行节点资源分别管理，并随请求冻结，恢复时不能更改。

每轮的正式结果引用保存于过程收据，`new_formal_result=true` 表示产生了新的正式结果。选定结果仍是开发研究，不自动成为最终样本外结论。开发循环拒绝最终test/holdout计算，输入和评价不得越过开发截止；最终研究应在候选冻结后单独执行。

[两轮研究包示例](../examples/package_campaign/README.md)在相同的六个证券日上比较两日和三日公式窗口。两轮分别计算、分别独立验证，窗口差异确实进入公式。研究指标是教学集中度均值，不用于宣称投资优劣。

恢复沿用候选原execution；已有Result继续缺失的验证或报告，已经完成的评价不重复运行。冻结基包、变体、输入声明或预算变化时拒绝恢复，新的研究应使用新的会话目录。

## 整体边界

参数研究要求基包、数据口径和独立Verifier已经准备完成。它支持在有限、可审阅范围内探索，不代表任意论文都能无人确认地转换为研究；固定公式复现仍以确认过的定义为准，不因开发指标更好而改写论文公式。

Qlib开发起点见[模型参数研究](../examples/package_campaign/README.md#qlib模型参数研究)。它每轮重新训练一个预声明模型，并封存处理器、模型和validation结果。允许2.0.0版split、fit、predict、fold-metrics，split显式采用development；最终selection/holdout节点不进入循环。
