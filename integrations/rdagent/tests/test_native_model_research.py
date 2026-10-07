"""上游模型组件按冻结响应运行，不连接数据库或付费服务。"""
import json
import sqlite3
import pytest
from quantwitness_rdagent.native_model_research import propose_model, reflect_model


def test_native_model_components_and_feedback(monkeypatch):
    def forbidden(*a, **k):
        raise AssertionError('不得创建SQLite缓存')
    monkeypatch.setattr(sqlite3, 'connect', forbidden)
    context={'confirmed_spec':'冻结模型定义','development':{'end':'2025-01-01'},'objective':{'metric':'mse'},'history':[], 'max_output_tokens':512}
    responses=iter([{'hypothesis':'跳连非线性模型','reason':'开发误差结构'}, {'definition':{'nodes':[{'inputs':[-1],'width':1,'activation':'identity'}]}},
        {'Observations':'已验证误差','Feedback for Hypothesis':'待比较','New Hypothesis':'增加隐层','Reasoning':'结构比较','Decision':False}])
    calls=[]
    def complete(prompt, **kwargs):
        calls.append({'prompt':prompt,**kwargs})
        return json.dumps(next(responses),ensure_ascii=False)
    proposal=propose_model(context,complete)
    assert proposal['hypothesis']=='跳连非线性模型'
    assert 'model tuning' in calls[0]['instructions'] and 'model' in calls[1]['instructions']
    assert 'definition' in calls[1]['instructions']
    context['current']={'candidate_id':'m','hypothesis':proposal['hypothesis'],'reason':proposal['reason'],'definition':json.loads(proposal['response'])['definition'],'status':'evaluated','metrics':{'value':0.5,'rows':1}}
    feedback=reflect_model(context,complete)
    assert feedback['new_hypothesis']=='增加隐层' and feedback['decision'] is False
    assert len(calls)==3 and all(c['max_output_tokens']==512 for c in calls)
