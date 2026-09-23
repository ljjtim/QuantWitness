# AI 从研究问题到结果的工作协议

## 唯一状态机

```text
需求冻结
  → 能力和数据发现
  → 创建 ResearchPackage
  → package lint
  → package admit
  → run / inspect / resume / retry-node / rerun-from
  → Result
  → verify
  → VerificationResult
  → report / compare / export-result / Dashboard
```

AI 不创建项目 runner，不手写内部 drift bundle，不猜 Catalog、PIT 或计划 hash，也不把 `local_only` 写成已经发布。

## 每个阶段的决策

### 需求冻结

一次性整理研究口径。仍缺用户事实时，列出缺口和建议；不会改变研究含义的默认值才可自动填入。

### 发现

使用 `capabilities`、`operator`、`artifact`、`recipe` 和 `catalog`。Catalog 搜索结果中的
`catalog_context.lock_reference` 可直接交给 lint/admit，dataset 的 `bindings` 与
`catalog_context.source_profiles` 说明各 profile/environment 需要的只读路径；AI 不自行猜路径。
输出只是发现信息，不是运行资格。

### 算子选择

1. 同语义算子已存在：复用。
2. 当前研究需要的新算法：项目 extension。
3. 已有两个异构真实消费者、独立 oracle、前视/标签/边界攻击测试且合同无项目语义：人工评估晋级 core。

只有第 3 类才进入唯一 OperatorDefinition 清单；不自动评分，不保留旧 ID 转发。

### lint

一次读取结构化结果：声明、字段、算子图、ResultSpec、指标、来源、已声明资源预算、缺失项和下一条命令。lint 失败就修改 package 或 bundle，不进入数据运行。

### admit

只提供当前 package、Catalog Lock、显式只读数据源和新输出目录。admit 自动构造漂移/PIT 证明和计划内 bundle 闭包。数据不可证明时保持 NOT_RUN。

直接采集的 raw 分钟查询无需额外来源版本清单。AI 必须让 admit 校验 completed bar、当前
Catalog 的 `raw + collected + pit_allowed` 语义、实际 schema、有界时区范围和稳定排序；
Runtime 再记录本次实际来源与读取内容身份。复权分钟和历史截面状态缺 PIT 快照时仍不进入
run。分钟 `AUDIT` 只能做扫描、重采样等观察，不能流入 Feature、Label、Signal、Statistics、
Validity 或交易仿真。

### run 与恢复

run 固定 plan、clock、seed、mode 和资源额度。恢复先看 `inspect` 的唯一建议：中断用 resume；只有错误码受 retry policy 允许且仍有余额时用 retry-node；终态分叉用 rerun-from；身份变化或 Result/package 合同失败则人工修改 package、重新 lint/admit 并新建 run。Runtime succeeded 后还要看独立 finalize 状态；旧 run 显示 `unknown`，不能据此声称已有 Result。

### verify 与消费

Result 是自包含计算结果；VerificationResult 是独立复验结论。报告、比较、导出和 Dashboard 必须同时绑定二者，不能只看 Runtime 的 succeeded。真正复现仍从 package lint/admit → run → verify 重新执行研究。

## 项目算子最短闭环

```powershell
python -m research_pipeline operator scaffold --output <新算子目录> --project-id <项目ID> --operator-id <算子ID> --format json
python -m research_pipeline operator validate --spec <operator.yaml> --source <源码目录> --format json
python -m research_pipeline operator build --spec <operator.yaml> --source <源码目录> --output <bundle父目录> --format json
python -m research_pipeline package lint --package <包目录> --extension-bundle <bundle目录> --json
python -m research_pipeline package admit --package <包目录> --extension-bundle <bundle目录> --catalog-lock <Catalog-Lock> --data-db <只读DuckDB> --output <计划目录> --json
```

scaffold 只提供可原样 validate/build 的 typed identity 示例，不是研究算法。项目入口 ABI 固定为
`run(context, inputs, output_root)`：`context` 提供项目/运行/节点/attempt、带时区 clock、seed 和参数；
`inputs` 是 Worker 已复验的 typed 输入；源码只能在 `output_root` 下写文件，并返回与声明端口、
Artifact 类型、相对路径、内容 hash、schema hash 和字节数一致的 commit。Worker 会拒绝未声明文件、
端口或类型不闭合及内容漂移。

项目源码的贴身测试至少覆盖：手算 oracle、PIT、标签隔离、时间边界、空/重复/缺失数据、确定性和负控制。计划发布后，run 和恢复只消费计划内闭包。

## 校验运行原则

运行任何检查前先回答：它会发现什么具体失败；失败后下一步会改什么。答不上来就不运行。相同事实只校验一次并让下游直接消费，不用评分表替代明确判断。
