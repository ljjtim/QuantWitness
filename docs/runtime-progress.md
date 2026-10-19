# 查看真实运行进度

```powershell
python -m research_pipeline inspect --run-root <运行目录> --json
```

`liveness.progress` 来自运行目录中的 `runtime-liveness.json`。节点自己的 `liveness` 也带同一观察投影。该文件记录进程存活、阶段和实际完成量；计划、checkpoint、Result、金融身份及恢复资格继续使用原有正式证据。

| 字段 | 含义 |
| --- | --- |
| `stage` | 当前观察阶段：`causal_batches` 为执行因果批，`causal_assembly` 为批完成后的输出组装；其他阶段沿用 Runtime 阶段 |
| `dag.completed / total` | 已通过原有门禁并成功提交或复用的节点数 / 冻结 DAG 节点总数 |
| `work.completed / total` | 已完成 Worker 返回、核心事实检查和输出封口的因果批数 / 按同窗规则及每批最多 8192 键计算的批总数 |
| `last_progress_at` | 对应计数最近一次增加的 UTC 时间；进度顶层为 DAG 和当前批的最近有效进展时间，尚无完成事件时为空 |
| `throughput_per_second` | 本次计数观察开始以来，每秒确认完成的单位数，包含等待和处理耗时 |
| `eta_seconds` | 当前因果批阶段的剩余秒数经验区间；至少两个有效批耗时样本后提供，使用最近最多八次样本的最小和最大单位耗时 |
| `status` | 当前可计量工作进度的观察状态，与进程 `health` 分开 |

心跳刷新 `heartbeat_at`，不增加计数，也不更新 `last_progress_at`。新节点清空上一节点的工作批计数，保留 DAG 计数。恢复时从原有成功 checkpoint 的校验与对账结果重新建立 DAG 计数，不从观察文件恢复执行状态。

## 状态含义

- `advancing`：当前因果批已有确认完成事件，距离最近进展尚未达到停滞阈值。
- `stalled`：进程仍在执行，但距离最近确认完成已超过 60 秒和近期最长单位耗时三倍中的较大值。它表示计数暂未前进，不等同于死锁或运行失败。
- `unknown`：没有当前工作批的有效完成事件，或当前阶段没有可计量的内部工作。例如模型拟合期间只显示阶段与 DAG 计数，不生成模型训练百分比。
- `waiting`：等待资源。
- `completed`：当前工作批全部完成；输出组装、checkpoint、Result 发布可能仍在继续，不代表整个研究已完成。
- `inactive`：owner 已停止或进程身份已不存活。

## 已知限制

ETA 仅估计当前因果批阶段，不含后续输出组装、其他 DAG 节点及独立验证。该区间反映已观察耗时范围，不是置信区间；工作负载变化时可能超出；当前批用时超过已观察最长批时，停止显示ETA，直到下一次完成事件取得新样本。停滞、等待、停止及缺少足够样本时不展示 ETA。DAG 节点的工作量不同，只提供节点吞吐，不推算整次运行 ETA。首个长批返回前保持 `unknown`，不根据心跳猜测内部进度。

当前计数不表示数据读取行数、字节数或模型迭代次数。因果批中的读取、计算与核心封口合计为一个完成单位。

## 文件与合成验证

```powershell
$env:PYTHONPATH = "research_pipeline/src;research_pipeline/tests"
python -m pytest research_pipeline/tests/test_runtime_progress.py -q
```

该测试文件使用临时 JSON、checkpoint、合成 Parquet 与本地 Worker，覆盖计数、吞吐、ETA、停滞判断、节点切换、失败重试与 checkpoint 复用；不访问数据库。
