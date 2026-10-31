"""交易候选复用因果Feature，但不放宽分析图的研究语义。"""
from types import SimpleNamespace

import pytest

from research_pipeline.packages.operator_graph import _validate_research_semantic_graph
from research_pipeline.packages.models import ResearchPackageError


def graph(*, consumers=('strategy',), label_source=False):
    def node(name, kind, inputs=(), parameters=None):
        return SimpleNamespace(node_id=name,operator_id=kind,operator_version='1.0.0',inputs=inputs,parameters=parameters or {})
    def link(source, port='signals'):
        return SimpleNamespace(input_port=port,source_node_id=source,source_output_port='out')
    nodes=[node('feature','feature', (link('label'),) if label_source else ())]
    for name in consumers:
        nodes.append(node(name,'simulation' if name=='strategy' else 'analysis',(link('feature'),),
                          {'strategy_declaration':{}} if name=='strategy' else {}))
    if label_source:
        nodes.append(node('label','label'))
    outputs={'feature':'research.feature-set.v1','simulation':'research.minute-simulation.v1','analysis':'research.minute-statistics.v1','label':'research.label.v1'}
    registry=SimpleNamespace(require_operator=lambda name,version: SimpleNamespace(output_ports=(SimpleNamespace(port='out',artifact_type=outputs[name]),)))
    return SimpleNamespace(nodes=nodes),registry


def test_execution_only_feature_does_not_require_future_label():
    recipe,registry=graph()
    _validate_research_semantic_graph(recipe,admission=registry,semantics=None)


@pytest.mark.parametrize('consumers',[(),('analysis',),('strategy','analysis')])
def test_analysis_feature_still_requires_declared_semantics(consumers):
    recipe,registry=graph(consumers=consumers)
    with pytest.raises(ResearchPackageError,match='research_semantics'):
        _validate_research_semantic_graph(recipe,admission=registry,semantics=None)


def test_execution_signal_cannot_hide_label_ancestor():
    recipe,registry=graph(label_source=True)
    with pytest.raises(ResearchPackageError):
        _validate_research_semantic_graph(recipe,admission=registry,semantics=None)
