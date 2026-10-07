"""上游 APIBackend 工厂的显式文本后端；预算和回执由调用者承担。"""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
import json
from threading import RLock


_REQUEST = ContextVar("quantwitness_native_request", default=None)
_BACKEND_LOCK = RLock()


class ResearchTextBackend:
    """只实现本阶段使用的无会话文本请求，不创建上游聊天缓存。"""

    def build_messages_and_create_chat_completion(
        self, user_prompt, system_prompt, *, json_mode=False,
        json_target_type=None, **kwargs,
    ):
        binding = _REQUEST.get()
        if binding is None:
            raise RuntimeError("原生研究请求尚未绑定显式模型客户端")
        if kwargs:
            raise ValueError("原生研究文本后端不支持额外请求参数")
        complete, max_output_tokens = binding
        if json_mode:
            user_prompt += "\n\n本次任务约束：\n" + system_prompt
            user_prompt += "\n\n输出要求：按任务指定字段只返回一个有效JSON对象，不输出Markdown代码围栏、标题或对象外的说明。"
        response = complete(
            user_prompt, instructions=system_prompt,
            max_output_tokens=max_output_tokens,
        )
        text = response.get("text") if isinstance(response, dict) else response
        if not isinstance(text, str) or not text.strip():
            raise ValueError("研究模型必须返回非空文本")
        if json_mode:
            value = json.loads(text)
            if not isinstance(value, dict):
                raise ValueError("研究模型必须返回JSON对象")
        return text

    def create_embedding(self, *args, **kwargs):
        raise NotImplementedError("本阶段未接入embedding模型")

    def build_chat_session(self, *args, **kwargs):
        raise NotImplementedError("原生研究使用显式无缓存文本请求")


@contextmanager
def research_backend(complete, max_output_tokens):
    """在单次同步原生调用内切换工厂，并在失败时恢复上游设置。"""
    from rdagent.oai.llm_conf import LLM_SETTINGS

    if not callable(complete):
        raise TypeError("complete必须是预算受控的文本请求函数")
    if type(max_output_tokens) is not int or not 1 <= max_output_tokens <= 8192:
        raise ValueError("单次输出token预算必须为1至8192")
    with _BACKEND_LOCK:
        previous = LLM_SETTINGS.backend
        token = _REQUEST.set((complete, max_output_tokens))
        try:
            LLM_SETTINGS.backend = "quantwitness_rdagent.native_backend.ResearchTextBackend"
            yield
        finally:
            LLM_SETTINGS.backend = previous
            _REQUEST.reset(token)
