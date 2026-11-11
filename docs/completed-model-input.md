# 把已训练模型接入新研究


模型导入用于复用已经完整封存的训练产物。
新研究负责预测和评价，来源训练记录保持原样。

## 开始前

- 准备可信本地来源运行目录。
- 取得完整 split 和 fit 工件身份。
- 取得来源 fit 和 split 节点标识。
- 保留模型、处理器、权重、源码及窗口文件。

`split` 工件保存样本划分。`fit` 工件保存训练产物。
计算步骤图（DAG）规定节点依赖。
正式结果包（Result）保存本次预测与评价。

## 接入流程

1. 在新研究包中声明 `research.model.import`。
2. 填入来源运行、工件身份和节点标识。
3. 将导入输出连接到预测和评价节点。
4. 准入新研究包。
5. 执行研究并检查正式 Result。

算子版本 `1.0.0` 独立于框架版本。
按时点可见（PIT）和最终留出集规则继续生效。

`research.model.import`（1.0.0）接收可信本地研究封存的完整split与fit工件，输出`splits: research.model-split-manifest.v2`和`models: research.model-fits.v2`。新DAG可继续predict、fold-metrics、selection及统计。不需要重复执行Feature、Label、split或fit。

## 声明导入节点

在新研究包的计算图中声明节点：

```yaml
operator_id: research.model.import
operator_version: 1.0.0
parameters:
  source_run_root: <来源运行目录的绝对路径>
  source_fit_artifact: <完整fit工件身份>
  source_split_artifact: <完整split工件身份>
  source_fit_node_id: model_fit
  source_split_node_id: model_split
```

## 准备和执行规则

正式准备读取来源 1 次。准备检查完整封存模型、候选与 fold 集合、配置和依赖。准备还检查训练可见性和来源执行事实。

新计划的节点参数冻结 `source_manifest_json`。声明编译仅校验字段和引用一致性，不在lint阶段扫描模型内容。已有冻结 manifest 在准备时完整复验。

计划读取与恢复只校验冻结声明和引用一致性。执行在复制消费前完整复验来源 1 次。执行仅消费冻结来源，不能替换路径或模型。

## 来源要求

来源运行允许保留资源验收失败状态。来源必须存在完成计算且封口的工件。原失败状态、实现、输入、训练边界及资源记录原样保留。

导入不是旧checkpoint成功复用，不补写旧成功状态，也不将当前性能改进追记为来源训练性能。缺少模型、处理器、权重、源码、窗口或来源证明时拒绝导入。

## 输出

Result 将模型索引表绑定导入节点的models端口，使用`research.qlib-model-inventory.v1`收集完整恢复依赖。原模型与split元数据不重写，来源训练与本次预测、评价执行分开记录。最终留出与PIT规则继续沿当前主链验收。


## 错误处理

| 现象 | 处理方式 |
| --- | --- |
| 缺少来源文件或证明 | 恢复完整来源运行，重新准入新研究。 |
| 来源身份变化 | 使用冻结来源，或按新来源创建新计划。 |
| 留出评价拒绝 | 检查当前系列账本和实际暴露区间。 |
