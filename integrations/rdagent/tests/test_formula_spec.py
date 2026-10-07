"""公式规格的出处、确认与付费恢复边界；全部使用离线材料。"""
import copy
import json
import sys

import pytest

from quantwitness_rdagent import formula_spec as spec
from quantwitness_rdagent import model_client
from quantwitness_rdagent.contracts import write_json, FrozenRequest
from quantwitness_rdagent.generation import build_prompt as code_prompt
from test_live_generation import live_payload


@pytest.fixture
def material():
    return {'source_title':'测试研报','source_id':'test-source','snapshot_artifact_id':'snapshot-test',
            'snapshot_manifest_hash':'existing-identity','pages':[{'pdf_page':4,'lines':['前15分钟剔除','过去20天均值']}],
            'review_notes':['图表需要人工审阅']}


@pytest.fixture
def draft():
    return {'schema_version':'paper-formula-draft-v1','title':'测试公式',
            'rules':[{'rule_id':'r1','origin':'paper_explicit','statement':'剔除前15分钟',
                      'evidence_refs':[{'pdf_page':4,'line_start':1,'line_end':1,'quote':'前15分钟剔除'}]}],
            'ambiguities':[{'ambiguity_id':'a1','question':'时间端点？','options':['起点','终点'],
                            'recommendation':'终点','impact':'事件不同','evidence_refs':[]}],
            'limitations':['文本限定范围']}


@pytest.fixture
def decision():
    return {'accepted_rule_ids':['r1'],'resolutions':[{'ambiguity_id':'a1','decision':'终点','rationale':'供应商定义'}],
            'interface':'daily_value(rows), rolling_value(rows)','review_notes':'已对照原文，端点属于项目决定'}


@pytest.fixture
def files(tmp_path, monkeypatch, material, draft, decision):
    monkeypatch.setattr(spec,'verify_materials',lambda value:None)
    paths=[tmp_path/name for name in ('draft.json','materials.json','decisions.json','confirmation.json')]
    for path,value in zip(paths,(draft,material,decision)):
        write_json(path,value)
    return paths


@pytest.mark.parametrize('fault',['page','quote','lines','missing','approved','origin','uncited','duplicate'])
def test_draft_rejects_wrong_source_and_structure(material,draft,fault):
    ref=draft['rules'][0]['evidence_refs'][0]
    if fault=='page': ref['pdf_page']=5
    elif fault=='quote': ref['quote']='不存在的引文'
    elif fault=='lines': ref['line_end']=3
    elif fault=='missing': del draft['title']
    elif fault=='approved': draft['approved']=True
    elif fault=='origin': draft['rules'][0]['origin']='confirmed'
    elif fault=='uncited': draft['rules'][0]['evidence_refs']=[]
    else: draft['rules'].append(copy.deepcopy(draft['rules'][0]))
    with pytest.raises(ValueError): spec.validate_draft(draft,material)


def test_project_decision_and_review_are_explicit(material,draft,files):
    draft['rules'][0].update(origin='project_decision',evidence_refs=[])
    spec.validate_draft(draft,material)
    write_json(files[0],draft)
    text=spec.review_spec(*files[:2])
    assert 'project_decision' in text and '待人工确认' in text and '时间端点' in text


def test_confirmation_requires_human_and_complete_decisions(files,decision):
    with pytest.raises(ValueError,match='explicit_human'):
        spec.confirm_spec(*files,confirmed_by='test')
    decision['resolutions']=[]
    write_json(files[2],decision)
    with pytest.raises(ValueError,match='unresolved'):
        spec.confirm_spec(*files,confirmed_by='test',approve=True)


def test_unconfirmed_and_changed_content_cannot_render(files,draft):
    with pytest.raises(FileNotFoundError): spec.render_spec(*files)
    spec.confirm_spec(*files,confirmed_by='test',approve=True)
    draft['title']='变更公式'
    write_json(files[0],draft)
    with pytest.raises(ValueError,match='changed_or_missing'): spec.render_spec(*files)


def test_confirmed_formula_enters_existing_frozen_request(files,tmp_path):
    spec.confirm_spec(*files,confirmed_by='test',approve=True)
    formula=spec.render_spec(*files)
    payload=live_payload(tmp_path/'code')
    payload['code_generation']['formula']=formula
    request=FrozenRequest.from_dict(payload)
    request.freeze()
    assert formula in json.loads(code_prompt(request).split('\n',1)[1]).values()
    assert '项目决定' in formula and 'existing-identity' in formula


def prepare_model(monkeypatch,material,fn):
    monkeypatch.setattr(spec,'extract_materials',lambda *args:material)
    monkeypatch.setattr(model_client,'public_config',lambda path:{'model':'test-model','base_url':'https://test.invalid/v1'})
    monkeypatch.setattr(model_client,'request_text',fn)


def invoke(tmp_path,**kwargs):
    return spec.extract_spec('package','archive','source',[4],tmp_path/'out','unused.env',**kwargs)


def test_reserved_before_request_and_success_reused(tmp_path,monkeypatch,material,draft):
    calls=[]
    def request(*args,**kwargs):
        receipt=spec.load(tmp_path/'out/model-calls/call-0000.json')
        assert receipt['status']=='reserved' and kwargs['instructions']==spec.INSTRUCTIONS
        calls.append(args)
        return {'model':'test-model','text':json.dumps(draft),'usage':{'output_tokens':30}}
    prepare_model(monkeypatch,material,request)
    assert invoke(tmp_path)==draft
    assert invoke(tmp_path)==draft
    assert len(calls)==1
    assert spec.load(tmp_path/'out/extraction.json')['semantic_review']=='pending'


@pytest.mark.parametrize('status',['reserved','failed'])
def test_uncertain_paid_call_never_repeated(tmp_path,monkeypatch,material,draft,status):
    prepare_model(monkeypatch,material,lambda *a,**k:pytest.fail('不应请求'))
    write_json(tmp_path/'out/model-calls/call-0000.json',{'status':status})
    with pytest.raises(ValueError,match='no_retry'): invoke(tmp_path)


def test_explicit_repair_and_two_call_budget(tmp_path,monkeypatch,material):
    calls=[]
    def request(*args,**kwargs):
        calls.append(args)
        return {'model':'test-model','text':'not json','usage':{'output_tokens':4}}
    prepare_model(monkeypatch,material,request)
    with pytest.raises(ValueError,match='json_invalid'): invoke(tmp_path)
    with pytest.raises(ValueError,match='explicit_repair'): invoke(tmp_path)
    with pytest.raises(ValueError,match='json_invalid'): invoke(tmp_path,repair=True)
    with pytest.raises(ValueError,match='budget_exhausted'): invoke(tmp_path,repair=True)
    assert len(calls)==2 and '上次原始响应' in calls[-1][1]


def test_repair_success_and_oversized_response_not_reused(tmp_path,monkeypatch,material,draft):
    replies=iter(['broken',json.dumps(draft)])
    prepare_model(monkeypatch,material,lambda *a,**k:{'model':'test-model','text':next(replies),'usage':{'output_tokens':50}})
    with pytest.raises(ValueError): invoke(tmp_path)
    assert invoke(tmp_path,repair=True)==draft
    other=tmp_path/'large'
    prepare_model(monkeypatch,material,lambda *a,**k:{'model':'test-model','text':json.dumps(draft),'usage':{'output_tokens':9000}})
    for _ in range(2):
        with pytest.raises(ValueError,match='output_exceeded'): invoke(other)


def test_changed_materials_stop_before_model(tmp_path,monkeypatch,material,draft):
    prepare_model(monkeypatch,material,lambda *a,**k:{'model':'test-model','text':json.dumps(draft),'usage':{}})
    invoke(tmp_path)
    material['pages'][0]['lines'][0]='不同内容'
    with pytest.raises(ValueError,match='configuration_changed'): invoke(tmp_path)


def test_cli_extract_does_not_import_rd(tmp_path,monkeypatch):
    from quantwitness_rdagent.__main__ import main
    calls=[]
    monkeypatch.setattr(spec,'extract_spec',lambda *a,**k:calls.append((a,k)))
    monkeypatch.setattr(sys,'argv',['rd','spec-extract','--package','p','--source-archive-root','a',
        '--source-id','s','--pages','4','5','6','--output',str(tmp_path),'--model-env-file','private.env'])
    main()
    assert calls[0][0][3]==[4,5,6]
    assert 'rdagent.core.conf' not in sys.modules


def test_model_failure_spends_reservation_without_exposing_error(tmp_path,monkeypatch,material):
    def request(*args,**kwargs):
        raise RuntimeError('private-value-must-not-appear')
    prepare_model(monkeypatch,material,request)
    with pytest.raises(RuntimeError,match='不自动重试'): invoke(tmp_path)
    receipt=(tmp_path/'out/model-calls/call-0000.json').read_text(encoding='utf-8')
    assert 'private-value-must-not-appear' not in receipt
    assert json.loads(receipt)['status']=='failed'
    with pytest.raises(ValueError,match='no_retry'): invoke(tmp_path,repair=True)


@pytest.mark.parametrize('changed',['decisions','materials'])
def test_changed_confirmation_inputs_cannot_render(files,decision,material,changed):
    spec.confirm_spec(*files,confirmed_by='test',approve=True)
    if changed=='decisions':
        decision['resolutions'][0]['decision']='起点'
        write_json(files[2],decision)
    else:
        material['snapshot_manifest_hash']='changed-identity'
        write_json(files[1],material)
    with pytest.raises(ValueError,match='changed_or_missing'): spec.render_spec(*files)


def test_reuse_preserves_human_edited_draft(tmp_path,monkeypatch,material,draft):
    prepare_model(monkeypatch,material,lambda *a,**k:{'model':'test-model','text':json.dumps(draft),'usage':{}})
    invoke(tmp_path)
    edited=copy.deepcopy(draft)
    edited['title']='人工修订规格'
    write_json(tmp_path/'out/draft.json',edited)
    with pytest.raises(ValueError,match='existing_draft_differs'): invoke(tmp_path)
    assert spec.load(tmp_path/'out/draft.json')==edited


def test_rendered_resolution_retains_question_and_options(files,decision):
    decision['resolutions'][0]['decision']='采用第一项'
    write_json(files[2],decision)
    spec.confirm_spec(*files,confirmed_by='test',approve=True)
    text=spec.render_spec(*files)
    assert '时间端点？' in text and '起点；终点' in text and '采用第一项' in text
