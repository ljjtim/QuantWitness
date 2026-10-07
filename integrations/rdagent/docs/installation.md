# RD-Agent 可选包的安装与贡献验收

核心 `quantwitness` 与可选集成 `quantwitness-rdagent` 分别发行。核心 wheel 不包含 RD-Agent 集成；可选集成的 wheel 包含 Python 模块，sdist 另含教学示例、文档和场景依赖清单。示例文件也随公开源码分发。

## 从发行文件开始

使用 Python 3.12 和独立虚拟环境。以下 Linux 命令从公开 QuantWitness 源码根执行，`WORK` 指向仓库外的新目录。Windows 可以使用 `Scripts/python.exe` 执行同一验收工具；本页完整发行验收以 Linux Python 3.12 为准。

```bash
WORK=/path/to/quantwitness-acceptance
python3.12 -m venv "$WORK/build-env"
"$WORK/build-env/bin/python" -m pip install build 'setuptools>=68' wheel
"$WORK/build-env/bin/python" tools/build_release_artifacts.py --project . --output "$WORK/core-dist"
"$WORK/build-env/bin/python" -m build --no-isolation --wheel --sdist \
  --outdir "$WORK/rdagent-dist" integrations/rdagent
python3.12 -m venv "$WORK/wheel-env"
"$WORK/wheel-env/bin/python" -m pip install "$WORK"/core-dist/*.whl "$WORK"/rdagent-dist/*.whl "pyarrow==21.0.0" "pypdf==6.10.0"
"$WORK/wheel-env/bin/python" -I tools/verify_rdagent_install.py --project . --output "$WORK/wheel-acceptance"
```

Ubuntu 缺少 `ensurepip` 时，安装对应的 `python3.12-venv` 系统包；也可使用已有 pip 的 `--python` 参数为已经创建的空虚拟环境安装 pip。虚拟环境必须保持 `include-system-site-packages = false`。示例 bundle 固定 `pyarrow==21.0.0`，安装命令应保留该版本约束。安装依赖需要访问包索引；公式运行阶段不需要网络、密钥或数据库。

sdist 单独用另一个空环境验收：

```bash
python3.12 -m venv "$WORK/sdist-env"
"$WORK/sdist-env/bin/python" -m pip install "$WORK"/core-dist/*.tar.gz "$WORK"/rdagent-dist/*.tar.gz "pyarrow==21.0.0" "pypdf==6.10.0"
"$WORK/sdist-env/bin/python" -I tools/verify_rdagent_install.py --project . --output "$WORK/sdist-acceptance"
```

每次输出目录必须不存在。工具确认两个业务包从当前虚拟环境导入，执行 `pip check`、两个公共 CLI 帮助、规格确认和 `request-build`，再使用安装包内的 worker 完成合成 Parquet 输入的 `lint/admit/run/verify/report`。第一候选公式故意偏移，预期独立验证失败；第二候选预期通过。2400 条分钟、10 个证券日的结果与 VerificationResult 路径保存在 `acceptance.json`。确认署名为 `synthetic-test-fixture`，不代表对真实研究材料的人工审阅。

公式安装验收不安装上游 RD-Agent，不执行其 Loop，也不调用模型。继续验证实际研究调度时，在同一 wheel 环境按[集成入口](../README.md)准备固定上游源码，再执行：

```bash
"$WORK/wheel-env/bin/python" -m pip install -r integrations/rdagent/requirements-linux.txt
export PYTHONPATH="$RD_AGENT_SOURCE"
export LITELLM_LOCAL_MODEL_COST_MAP=True
"$WORK/wheel-env/bin/python" integrations/rdagent/examples/prediction_campaign/prepare.py --output "$WORK/prediction-campaign"
"$WORK/wheel-env/bin/python" -m quantwitness_rdagent campaign-run --request "$WORK/prediction-campaign/request.json"
"$WORK/wheel-env/bin/python" -m quantwitness_rdagent campaign-resume --request "$WORK/prediction-campaign/request.json"
```

这里只把固定 RD-Agent 上游源码加入 `PYTHONPATH`，核心与可选集成继续使用安装包。预期两轮后选择 `half`，开发MSE从0.0000255降至0；恢复不追加评价。完整公式Loop见[第二公式示例](../examples/volume_concentration/README.md)，校准研究边界见[两轮教学例](../examples/prediction_campaign/README.md)。场景依赖清单覆盖这里使用的调度链，不安装上游所有其他场景。

## Qlib 机器学习发行包验收

需要训练模型时，另建空环境安装核心 wheel 的 `ml` 可选依赖。Python 3.12 是本轮安装验收版本，示例从公开源码根执行：

```bash
python3.12 -m venv "$WORK/ml-env"
WHEEL=$(find "$WORK/core-dist" -maxdepth 1 -name '*.whl' -print -quit)
"$WORK/ml-env/bin/python" -m pip install "${WHEEL}[ml]" "pyarrow==21.0.0"
"$WORK/ml-env/bin/python" -I tools/verify_ml_install.py --output "$WORK/ml-acceptance"
```

Windows PowerShell 使用同一工具，`$Work` 指向研究盘上的构建目录：

```powershell
python -m venv "$Work/ml-env"
$Wheel = (Get-ChildItem "$Work/core-dist/*.whl").FullName
& "$Work/ml-env/Scripts/python.exe" -m pip install "${Wheel}[ml]" "pyarrow==21.0.0"
& "$Work/ml-env/Scripts/python.exe" -I tools/verify_ml_install.py --output "$Work/ml-acceptance"
```

工具在仓库外的输出目录运行，检查安装来源和 `pip check`，在合成样本上分别训练 Qlib LinearModel、LGBModel、XGBModel，并封存每个模型的 RobustZScoreNorm 与 Fillna 处理器。模型迁移到另一目录后，由独立进程使用不含标签的输入恢复预测，要求与原预测逐项完全一致。结果与依赖版本写入 `ml-acceptance/acceptance.json`。此验收只证明安装、训练和模型恢复，不产生正式 ResearchPackage Result，也不声明投资绩效；全程不访问数据库或模型 API。

本地发行验收已覆盖 Linux Python 3.12 和 Windows Python 3.10。CI 的 `ml-distribution` 作业覆盖 Ubuntu 和 Windows 的 Python 3.12 wheel 安装。远端执行状态以具体 CI 记录为准；其他 Python 版本、平台和 sdist 的 ML 训练覆盖不能由这项验收推定。
## 可认领贡献

| 任务 | 修改范围 | 验收方式 |
| --- | --- | --- |
| 增加一个可手算的新公式示例 | `examples/<新案例>`、文档与公开文件清单 | 正确公式通过独立验证；至少一个数值错误和一个可见时间错误失败；不依赖私有数据 |
| 改善安装报错与排障说明 | 本页、集成 README、安装验收工具 | 在空环境复现具体失败，文档命令能修复；保留真实错误和日志位置 |
| 补充公开请求构建边界回归 | `tests/test_request_builder.py` | 构造确认材料与目标来源不一致的输入，断言在建立研究 session 前拒绝 |
| 增加跨平台安装覆盖 | CI 和安装验收工具 | 新平台从 wheel 与 sdist 分别导入安装包并产生独立验证结果 |

开始贡献时在 issue 或 PR 里说明研究问题、拟修改范围和可运行验收命令。保持改动集中；无需为一个示例修改核心金融合同。提交源码、测试和文档，运行目录留在仓库外。更完整的项目边界和 PR 要求见[贡献指南](../../../CONTRIBUTING.md)。

## 验收边界

CI 中的 `rdagent-distribution` 作业执行 wheel/sdist 自动化安装验收，以及公开请求、公式审阅与开发预测研究的边界回归。工作流文件存在不代表远端已经运行；实际结果以对应 CI 运行记录为准。自动化验收也不代替首次接触框架的真人独立使用记录。

安装与研究输出包括虚拟环境、发行文件、合成输入、bundle、Result、VerificationResult、报告及命令日志，均保留在 `WORK` 下。需要清理时先保存要复核的收据和研究结果。
