"""生成网络真实拟合、生产源码封存和文件恢复。"""
import json
import shutil

import numpy as np
import pytest

from research_pipeline.research.modeling.qlib import fit_bundle, predict_bundle, normalize_candidates
from test_qlib_model_integration import synthetic_samples, candidate


def generated_candidate():
    item = candidate()
    item['candidate_id'] = 'generated_skip'
    item['model'] = {'class': 'GeneratedModel', 'module_path': 'research_pipeline.research.modeling.generated',
        'kwargs': {'definition': {'nodes': [
            {'inputs': [-1], 'width': 3, 'activation': 'tanh'},
            {'inputs': [-1, 0], 'width': 2, 'activation': 'relu'},
            {'inputs': [0, 1], 'width': 1, 'activation': 'identity'}]},
            'epochs': 3, 'learning_rate': 0.02, 'early_stop': 2, 'l2': 0.001}}
    return item


@pytest.fixture(autouse=True)
def no_database(monkeypatch):
    import sqlite3
    import duckdb
    def reject(*args, **kwargs):
        raise AssertionError('生成模型验收禁止数据库连接')
    monkeypatch.setattr(sqlite3, 'connect', reject)
    monkeypatch.setattr(duckdb, 'connect', reject)


@pytest.fixture(scope='module')
def generated_bundle(tmp_path_factory):
    root = tmp_path_factory.mktemp('generated')
    samples, _ = synthetic_samples()
    row = fit_bundle(samples.iloc[:390], samples.iloc[400:490], candidate=generated_candidate(),
        feature_columns=('x1', 'x2'), output_root=root, bundle_path='model', root_seed=7, fit_scope_ref='train')
    return root, row, samples


def test_generated_real_fit_prediction_and_relocation(generated_bundle, tmp_path):
    root, row, samples = generated_bundle
    first = predict_bundle(root, row, samples.iloc[500:600])
    altered = samples.iloc[500:600].copy()
    altered['target'] = 1e10
    np.testing.assert_array_equal(first, predict_bundle(root, row, altered))
    moved = tmp_path/'moved'
    shutil.copytree(root, moved)
    np.testing.assert_array_equal(first, predict_bundle(moved, row, altered))
    assert np.isfinite(first).all() and len(first) == 100


def test_generated_seals_production_source_and_weights(generated_bundle):
    from qlib.utils.serial import Serializable
    from research_pipeline.research.modeling.generated_definition import compile_source
    root, row, _ = generated_bundle
    config = json.loads((root / row['config_path']).read_text(encoding='utf-8'))
    source = (root / config['generated']['source_path']).read_text(encoding='utf-8')
    definition = config['candidate']['model']['kwargs']['definition']
    assert source == compile_source(definition, len(config['feature_columns']))
    model = Serializable.load(root / row['model_path'])
    assert model.source == source
    assert model.definition == definition
    with np.load(root / config['generated']['weights_path'], allow_pickle=False) as weights:
        assert set(weights.files) == set(model.weights)
        assert {name: list(weights[name].shape) for name in weights.files} == config['generated']['weight_shapes']
        for name in weights.files:
            np.testing.assert_array_equal(weights[name], model.weights[name])


def test_generated_restore_rejects_changed_sealed_weights(generated_bundle, tmp_path):
    root, row, samples = generated_bundle
    moved = tmp_path / 'changed-weights'
    shutil.copytree(root, moved)
    config = json.loads((moved / row['config_path']).read_text(encoding='utf-8'))
    path = moved / config['generated']['weights_path']
    with np.load(path, allow_pickle=False) as stored:
        weights = {name: stored[name].copy() for name in stored.files}
    name = next(iter(weights))
    weights[name].flat[0] += 0.1
    np.savez(path, **weights)
    with pytest.raises(ValueError, match='权重'):
        predict_bundle(moved, row, samples.iloc[500:501])


def test_generated_restore_rejects_modified_source(generated_bundle, tmp_path):
    root, row, samples = generated_bundle
    moved = tmp_path/'changed'
    shutil.copytree(root, moved)
    (moved/'model/network.py').write_text('raise RuntimeError()', encoding='utf-8')
    with pytest.raises(ValueError, match='源码'):
        predict_bundle(moved, row, samples.iloc[500:501])


@pytest.mark.parametrize('change', ['future_edge', 'output_width', 'unused_node', 'extra_fit', 'rank_label'])
def test_generated_contract_rejects_unsupported_model(change):
    item = generated_candidate()
    if change == 'future_edge':
        item['model']['kwargs']['definition']['nodes'][0]['inputs'] = [1]
    elif change == 'output_width':
        item['model']['kwargs']['definition']['nodes'][-1]['width'] = 3
    elif change == 'unused_node':
        item['model']['kwargs']['definition']['nodes'][-1]['inputs'] = [-1]
    elif change == 'extra_fit':
        item['fit'] = {'test': True}
    else:
        item['processors']['learn'].append({'class': 'CSRankNorm', 'kwargs': {}})
    with pytest.raises(ValueError):
        normalize_candidates([item])


def test_generated_declaration_does_not_load_optional_training_dependencies(tmp_path):
    import os
    from pathlib import Path
    import subprocess
    import sys
    root = Path(__file__).resolve().parents[1]
    script = '''import sys, importlib.abc
class Deny(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'qlib', 'torch'}:
            raise AssertionError('声明阶段不能加载可选模型依赖:'+fullname)
sys.meta_path.insert(0, Deny())
from research_pipeline.research.modeling.generated_definition import compile_source
from research_pipeline.research.modeling.qlib import normalize_candidates
compile_source({'nodes':[{'inputs':[-1], 'width':1, 'activation':'identity'}]}, 2)
'''
    env = dict(os.environ, PYTHONPATH=str(root / 'src'), PYTHONDONTWRITEBYTECODE='1')
    completed = subprocess.run([sys.executable, '-B', '-c', script], env=env, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stderr


def test_generated_mainline_dependency_preflight_records_torch():
    from research_pipeline.research.modeling.walk_forward import model_dependency_preflight
    receipt = model_dependency_preflight([generated_candidate()], thread_count=1)
    assert receipt['dependencies']['torch'].split('+')[0] == '2.5.1'
    assert receipt['dependencies']['pyqlib'] == '0.9.7'
