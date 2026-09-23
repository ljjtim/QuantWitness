"""Catalog 默认 Transform 与受信实现描述符的唯一绑定。"""

from __future__ import annotations

from research_pipeline.platform.builtin_transforms import lag_values

from .transforms import TransformDescriptor, describe_transform


LAG_CLOSE_PARAMETERS = {
    "type": "object",
    "properties": {"periods": {"type": "integer", "minimum": 1}},
    "required": ["periods"],
    "additionalProperties": False,
}
LAG_CLOSE_TIME_POLICY = {
    "node_type": "Lag",
    "output_port": "feature",
    "visibility": "at_or_before_input",
    "future_dependency": False,
}
LAG_CLOSE_RESOURCE_HINT = {
    "streaming": True,
    "lookback_rows": 1,
}


def build_mainline_transform_descriptors() -> tuple[TransformDescriptor, ...]:
    return (
        describe_transform(
            lag_values,
            transform_id="builtin.daily.lag-close.v1",
            implementation_version=2,
            inputs=("values",),
            outputs=("lagged_values",),
            parameters_schema=LAG_CLOSE_PARAMETERS,
            time_policy=LAG_CLOSE_TIME_POLICY,
            resource_hint=LAG_CLOSE_RESOURCE_HINT,
            input_schema={"values": {"data_type": "float64"}},
            output_schema={"lagged_values": {"data_type": "float64"}},
        ),
    )


__all__ = [
    "LAG_CLOSE_PARAMETERS",
    "LAG_CLOSE_RESOURCE_HINT",
    "LAG_CLOSE_TIME_POLICY",
    "build_mainline_transform_descriptors",
]
