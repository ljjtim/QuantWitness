# 安装RD-Agent可选集成

核心包为`quantwitness`。可选包为`quantwitness-rdagent`。可选包0.2.0依赖核心1.6.x。

安装器会拒绝核心小于1.6.0或不小于1.7的组合。研究调度使用固定上游RD-Agent源码。完整调度需要Linux环境。

Windows通过WSL运行调度。

## 准备环境

使用Python 3.12和独立虚拟环境。
在公开QuantWitness源码根执行命令。
设置以下环境变量为自己的目录。

| 变量 | 用途 |
| --- | --- |
| `WORK` | 仓库外的输出根目录 |
| `QUANTWITNESS_REPO` | 含src和integrations的公开源码根 |
| `RD_AGENT_SOURCE` | 固定上游源码目录 |
| `RD_AGENT_ENV` | Linux虚拟环境目录 |
| `RESEARCH_TEMP_ROOT` | 本次临时文件目录 |

```bash
git clone https://github.com/microsoft/RD-Agent.git "$RD_AGENT_SOURCE"
git -C "$RD_AGENT_SOURCE" checkout --detach 484776c211e4fbbeef03e0ec00d6bbee7362a4f4
python3.12 -m venv "$RD_AGENT_ENV"
export RD_AGENT_PYTHON="$RD_AGENT_ENV/bin/python"
"$RD_AGENT_PYTHON" -m pip install -r "$QUANTWITNESS_REPO/integrations/rdagent/requirements-linux.txt"
"$RD_AGENT_PYTHON" -m pip install --no-deps "$RD_AGENT_SOURCE"
"$RD_AGENT_PYTHON" -m pip install -e "$QUANTWITNESS_REPO" "pyarrow==21.0.0"
"$RD_AGENT_PYTHON" -m pip install -e "$QUANTWITNESS_REPO/integrations/rdagent[materials,live]"
export PYTHONPATH="$RD_AGENT_SOURCE:$QUANTWITNESS_REPO/integrations/rdagent/src"
export TMPDIR="$RESEARCH_TEMP_ROOT"
export LITELLM_LOCAL_MODEL_COST_MAP=True
"$RD_AGENT_PYTHON" -m quantwitness_rdagent --help
```

Ubuntu缺少ensurepip时，安装对应的python3.12-venv系统包。
虚拟环境不启用系统site-packages。
安装依赖需要访问包索引。
合成公式执行不需要网络、密钥或数据库。
第三方许可见[第三方说明](../../../THIRD_PARTY_NOTICES.md)。

## 配置付费模型

固定响应模式不使用模型配置文件。live模式显式传入`--model-env-file`。将配置保存在仓库外.env文件。

字段名为`MODEL`、`BASE_URL`、`API_KEY`和`PROXY_URL`。接口使用HTTPS，地址不携带凭据。PROXY_URL必须指定HTTP或HTTPS代理。

准备请求只读取模型公开身份，不调用接口。执行前核定请求中的调用和输出预算。

## 验收发行包

核心发行使用allowlist staging构建。
不要直接打包本地私有Catalog目录。
每个输出目录必须尚不存在。

```bash
python3.12 -m venv "$WORK/build-env"
"$WORK/build-env/bin/python" -m pip install build 'setuptools>=68' wheel
"$WORK/build-env/bin/python" tools/build_release_artifacts.py --project . --output "$WORK/core-dist"
"$WORK/build-env/bin/python" -m build --no-isolation --wheel --sdist --outdir "$WORK/rdagent-dist" integrations/rdagent
python3.12 -m venv "$WORK/wheel-env"
"$WORK/wheel-env/bin/python" -m pip install "$WORK"/core-dist/*.whl "$WORK"/rdagent-dist/*.whl "pyarrow==21.0.0" "pypdf==6.10.0"
"$WORK/wheel-env/bin/python" -I tools/verify_rdagent_install.py --project . --output "$WORK/wheel-acceptance"
python3.12 -m venv "$WORK/sdist-env"
"$WORK/sdist-env/bin/python" -m pip install "$WORK"/core-dist/*.tar.gz "$WORK"/rdagent-dist/*.tar.gz "pyarrow==21.0.0" "pypdf==6.10.0"
"$WORK/sdist-env/bin/python" -I tools/verify_rdagent_install.py --project . --output "$WORK/sdist-acceptance"
```

验收工具核对安装来源、依赖和公共命令。
工具使用合成Parquet完成准入、执行和报告。
教学候选包含预期失败与成功案例。
成功案例发布Result并与手算值比较。
验收收据保存在acceptance.json。
工具不调用付费模型，也不安装上游调度。

## 验证研究调度

安装固定上游后运行预测教学例。

```bash
"$RD_AGENT_PYTHON" integrations/rdagent/examples/prediction_campaign/prepare.py --output "$WORK/prediction"
"$RD_AGENT_PYTHON" -m quantwitness_rdagent campaign-run --request "$WORK/prediction/request.json"
"$RD_AGENT_PYTHON" -m quantwitness_rdagent campaign-resume --request "$WORK/prediction/request.json"
```

预期选择half，恢复不增加评价。公式调度见[合成公式示例](../examples/volume_concentration/README.md)。Qlib因子、模型和联合研究另需核心ml依赖。

可以在安装核心时选择`[ml]`。模型发行验收使用`tools/verify_ml_install.py`。工具检查训练、处理器封存和无标签预测恢复。

日频开发模型不需要LEAN。组合执行需要[LEAN安装](../../lean/README.md)。

## 输出与限制

虚拟环境、发行包、合成输入和报告位于WORK。
清理前保留Result、模型及实际依赖。
验收收据说明实际运行平台和依赖。
合成验收不证明投资效果或全平台兼容。
