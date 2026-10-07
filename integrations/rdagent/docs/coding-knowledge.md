# 编码经验复用

编码经验保存候选源码、明确的技术错误、修复实现和来源身份。生成提示只消费这些技术事实，金融表现由正式研究反馈链提供。

## 公式

公式复现请求中的 `coding_knowledge` 保留记录数和源码字符数预算：

```json
{
  "coding_knowledge": {
    "source_session": "I:/research/source/session",
    "retrieval_scope": "technical_transfer",
    "max_records": 4,
    "max_source_chars": 30000
  }
}
```

`retrieval_scope` 默认是 `exact_formula`，要求已确认公式和接口完全一致。`technical_transfer` 允许不同公式，但确认接口必须相同，并且源码或公式声明具有共同计算结构。检索从实际 Python 代码提取窗口切片、聚合、离散程度、极值、缺失处理、除法与条件判断；按相同任务优先，再选择结构适用的修复链。当前生成目标始终来自当前会话的确认公式。

来源会话必须完成正式独立验证和报告。每份成功候选再次核对 Result、公式覆盖、Verifier、已准入计划与已封存源码；失败候选只有明确的代码错误或公式合同错误才会进入经验。提示保留来源公式、来源候选、共同结构和对应成功修复。源码按完整文件计入预算，不截断函数。

CoSTEER 继续承担原有查询、生成、评价和签名持久化。冻结来源位于 `coding-knowledge/input.json`，每轮提示视图位于 `query-XXXX.json`；编码尝试回执保存实际消费的来源记录 ID。

## 模型

模型研究主循环通过 `ModelCodingKnowledge` 消费经验：

```python
from quantwitness_rdagent.model_knowledge import ModelCodingKnowledge
from quantwitness_rdagent.model_research_evidence import validate_coding_source

knowledge = ModelCodingKnowledge(
    session_root,
    task={"interface": "generated-dag-v1", "model_family": "GeneratedModel"},
    source_sessions=[source_session],
    validate_source=validate_coding_source,
    source_template=compiled_baseline,
)
records = knowledge.query(attempt)
```

当前会话的未修复编译失败会优先送入下一次修复提示。跨会话来源必须至少包含一个正式验证通过的实现；每份来源记录都由传入的正式核验函数检查。相同模型族的不同 `definition.nodes` 保存为不同任务结构，节点定义可通过 `record(..., task=...)` 与 `query(..., task=...)` 显式传入。同接口的跨模型经验依据共同库和代码结构选择。

生成模型成功后，`record_coding_success` 从正式 Result 复制已封存的 `network.py`。它核对独立验证、Result ID、Result 路径、已准入计划、开发模式、模型配置和提案节点定义。来源导入时，`validate_coding_source` 同时核对轮次提案、实际编译失败收据、执行引用和封存源码。成功记录的 `bundle_ref` 指向保存的源码副本；Result 中的封存源码仍是正式依据。

模型记录位于 `model-coding-knowledge/records/`，`input.json` 保存冻结来源和检索合同，`query-XXXX.json` 保存该次目标与提示视图。恢复读取这些冻结文件，不重新扫描来源；修改模型合同、来源、预算或已冻结查询目标须使用新会话或新尝试序号。

## 数据与适用范围

提示排除完整诊断正文、文件路径、实测行情、标签值和 test/holdout 金融指标，只保留允许的状态、错误类别及代码。资源耗尽、worker 崩溃、报告失败和验证基础设施失败不记成代码修复经验。来源的成功实现用于当前任务的编码参考，当前候选仍须走自己的正式执行和独立验证。

本路径使用显式本地来源和代码结构，不需要 embedding、向量数据库、SQLite 或额外模型调用。当前提供正式 GeneratedModel 模型来源核验；其他模型类型可通过同一接口接入自己的正式来源验证器。
