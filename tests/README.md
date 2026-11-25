# 如何选择和理解测试

测试用于发现具体错误。运行前先明确：哪种错误会使这条测试失败，失败后应该修正什么。

从包含 `pyproject.toml` 的源码根执行命令。安装 `.[test]`；模型测试按主 README 安装 `.[ml]` 或 `.[ml,ml-sequence]`。测试使用合成输入，临时输出放在仓库外；运行会创建测试库的用例前，按使用环境的写入规则取得许可。

## 按改动选择

局部修改直接运行对应文件或用例，不需要先跑全量。例如修改标签时间边界：

```powershell
python -m pytest -q tests/test_research_causal_time_contracts.py --basetemp ../quantwitness-test-work/causal
```

`--basetemp` 指定的目录会被 pytest 清空。每次运行使用专用目录，不得指向数据目录、结果目录或多个并行任务共用的目录。

重要计算需要数值对照。金融测试使用手算金额、数量和日期；模型使用固定合成输入、上游直接调用差分、封存后恢复预测及未来数据扰动。只检查形状、状态或文件存在，不能代替数值验收。

## 运行分组

默认 `python -m pytest` 仍收集当前源码中的全部核心测试，没有默认排除项。标记用于主动选择，允许一条用例属于多个组：

| 标记 | 范围 | 适用改动 |
| --- | --- | --- |
| `integration` | 已标注的真实子进程、主链、并行和恢复流程 | 调度、Worker、主链交接 |
| `distribution` | 已标注的构建、安装和发行清单测试 | 打包、资源或安装入口 |
| `ml` | 已标注的模型依赖、训练和Qlib计算测试 | 模型、处理器、序列、预测 |
| `archive_acceptance` | 历史验收记录自洽性 | 维护历史验收材料 |
| `external_factor` | 单仓库外部因子模块边界 | 单仓库因子发布集成 |

这些标记说明执行条件，不表示测试质量等级，也不保证未标记用例都是快速、纯内存测试。需要精确控制范围时使用文件或用例路径。

```powershell
# 调度与恢复组
python -m pytest -q -m integration --basetemp ../quantwitness-test-work/integration

# 模型组；先安装模型依赖
python -m pytest -q -m ml --basetemp ../quantwitness-test-work/ml

# 构建与安装组
python -m pytest -q -m distribution --basetemp ../quantwitness-test-work/distribution

# 全部核心测试，记录最慢用例和可保存的回执
python -m pytest -q --durations=20 --junitxml=../quantwitness-test-work/core.xml --basetemp ../quantwitness-test-work/core
```

可选 RD-Agent 使用独立入口：

```powershell
python -m pytest -q integrations/rdagent/tests --basetemp ../quantwitness-test-work/rdagent
```

核心全量不自动包含 RD-Agent、示例目录内测试或真实 LEAN 引擎验收。CI分别运行示例算法、核心公开合同、安装后的流程和可选集成。源码与安装后环境的重复运行用于发现导入或打包差异，不能当作新增的独立金融覆盖。

## 测试应证明什么

- 正向用例调用实际入口，核对独立预期结果。反向用例由生产逻辑拒绝输入，测试辅助函数不能代替生产逻辑抛出预期错误。
- mock 用于隔离外部服务或检测调用参数。替换了整个执行器的测试，只证明路由或交接；真实训练、回测和恢复另由集成用例证明。
- 计算预期值优先使用手算、已确认公式或独立上游实现。直接调用被测函数生成预期值只能检查一致性。
- 参数化应覆盖不同边界、规则或分支。只有名称不同、输入行为相同的案例可以合并。
- 配置测试检查依赖、权限、输入输出和发布身份；文档检查验证命令与链接。展示名称、中文措辞和内部函数拼写通常不是合同。
- 没有直接 `assert` 不一定无效：断言可能位于被调用的检查器中。调用后没有异常，也可以验证被拒绝过的合法输入能正常通过。

## 如何报告结果

分别记录通过、失败、跳过和未执行。通过数是参数化用例数量，不是独立功能或缺陷的数量。重跑只记录受影响用例；组合回归结果不能写成一次全量全绿。

检查点载荷完整性、DAG不重跑已提交节点、LEAN账户执行是不同覆盖。PIT通用时间合同也不能代替财报、行业、ST和成分股实际消费者的各自验证。历史验收JSON只证明保存的记录自洽，不能提高当前运行能力。

本目录的公开源码子集由 `tools/release_allowlist.py` 决定。新增支持模块和合成文件时同步清单；测试应能脱离维护者的内部研究目录被收集和执行。
