# 独立 Verifier 输入列投影

项目 Verifier 可在构建 bundle 时，为已授权 Result 表显式声明所需列。声明后的输入只复制这些列，未声明的授权表仍逐字节完整复制。正式 Result 保持完整，不修改原文件。

## 构建声明

```python
bundle = compile_project_verifier_bundle(
    source_root=source_root,
    output_root=bundle_root,
    project_id="research-example",
    verifier_id="research.check",
    verifier_version="1.0.0",
    entry_module="check",
    entry_function="verify",
    authorized_schema_ids=("project.example.rows.v1",),
    authorized_columns={
        "project.example.rows.v1": ("session", "instrument_id", "value"),
    },
    dependency_lock=dependency_lock,
)
```

`authorized_columns` 的键必须属于 `authorized_schema_ids`；列名列表必须非空、无重复，并按声明顺序输出。声明的是顶层 Arrow 字段，嵌套字段随所属顶层字段保留。每个分区都必须包含声明列，否则在启动 Worker 前拒绝。Verifier 读取未声明的必需列时失败，不能改用缺失值、抽样或跳过独立复算。

有列声明的 bundle 使用 `project-verifier-extension-bundle-v2`。声明进入 bundle 内容身份，改变声明必须重新构建并冻结相应身份。公开身份字段集合不变，既有 `bundle_hash` 绑定完整声明。未声明列的 bundle 保持 v1 格式和身份规则。

## 输入 manifest

列投影使用 `project-verifier-input-v2`，表条目包含：

| 字段 | 含义 |
| --- | --- |
| `schema_id`、`artifact_type` | 正式 Result 表类型 |
| `files` | 本次独立输入目录中的文件路径 |
| `columns` | 实际投影列及顺序；`null` 表示完整复制 |
| `source_table_manifest` | 原始 ResultTableManifest，包括表、工件、文件、schema、行数及已有摘要身份 |

投影文件的 schema 是原表选定字段；保留字段类型、可空性、字段元数据、行数和值及分区内行序。表级元数据不复制，避免携带对未选择列的描述。原始完整表身份与投影文件身份含义不同，`source_table_manifest` 描述原表，不能用于核验投影文件的字节相等性。

未使用列投影时，输入继续使用 `project-verifier-input-v1`，表条目和文件字节保持原样。

## 完整性与资源

投影只消费本次已验证 ResultSnapshot 中的授权表，正式 Result 的完整文件摘要、schema 和行数核对沿用 ResultStore。投影不会把全表完整性检查改成只检查所选列，也不改变独立金融重算、holdout、PIT 或结果复用合同。

Workspace 等准备阶段通过公开只读函数 `estimate_project_verifier_input_size(source, *, columns=None)` 获取 `{"bytes": int, "measurement_status": "measured" | "estimated"}`；调用方按 `manifest.authorized_columns.get(schema_id)` 选择列。完整复制返回 `measured`，投影返回 `estimated`。该入口复用内部函数 `_verifier_input_size(source, columns, guard=None)`，正式执行传入同一资源守卫。投影预估不是实际复制量下界，不能把原表完整大小标成投影下界。

完整模式按原文件实际字节预估空间；投影按所选列块的压缩字节及页脚开销估算，不再把被排除列的整份文件计入复制预算。估算还包括授权支持工件，磁盘剩余空间也在准备前检查。重编码后的实际大小可能与估计不同：写入器逐次限制准备文件的实际字节，现有资源守卫继续测量整个临时目录、进程内存、进程数和耗时。输入 manifest、上下文和 Worker 输出同样计入目录实测。任何资源超限均拒绝本次验证，不返回部分通过。

投影按固定 8,192 行批次、单线程读取和写入独立 Parquet 文件，检查批次前后的资源用量；没有整表物化、硬链接、共享可写来源或跨快照缓存。准备目录在成功或失败后清理。

## 有界宽表测量

纯文件合成测试使用固定随机种子生成 8,192 行、64 列的表，只声明一列。一次实际测量：

| 复制模式 | 文件字节 |
| --- | ---: |
| 完整文件 | 4,260,164 |
| 单列投影 | 34,029 |
| 减少比例 | 99.20% |

该测量描述此合成表的复制量；正式研究的收益取决于声明列数、数据类型及压缩情况。测试同时核对投影值和列、缺列拒绝、冻结身份、预算、空分区、原文件隔离及完整模式。

运行专属回归：

```powershell
python -m pytest -q research_pipeline/tests/test_project_verifier_inputs.py
```
