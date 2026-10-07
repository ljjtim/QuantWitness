"""已准入日频矩阵上的有限因果表达式；数值算子复用 Qlib。"""
from __future__ import annotations

import ast
from dataclasses import dataclass
import math
import operator
import re
from typing import Mapping, Sequence

import numpy as np
import pandas as pd


class QlibExpressionError(ValueError):
    error_code = "research_qlib_expression_invalid"


_UNARY = {"Abs", "Sign", "Log", "Not"}
_PAIR = {"Add", "Sub", "Mul", "Div", "Power", "Greater", "Less", "Gt", "Ge", "Lt", "Le", "Eq", "Ne", "And", "Or"}
_ROLLING = {"Mean", "Sum", "Std", "Var", "Max", "Min", "Med", "Mad", "Rank", "Count", "Slope", "Rsquare", "Resi", "IdxMax", "IdxMin", "Skew", "Kurt", "WMA"}
_SHIFT = {"Ref", "Delta"}
_PAIR_ROLLING = {"Corr", "Cov"}
_BINARY = {ast.Add: "Add", ast.Sub: "Sub", ast.Mult: "Mul", ast.Div: "Div", ast.Pow: "Power", ast.BitAnd: "And", ast.BitOr: "Or"}
_COMPARE = {ast.Gt: "Gt", ast.GtE: "Ge", ast.Lt: "Lt", ast.LtE: "Le", ast.Eq: "Eq", ast.NotEq: "Ne"}
_SCALAR_BINARY = {"Add": operator.add, "Sub": operator.sub, "Mul": operator.mul, "Div": operator.truediv, "Power": operator.pow, "And": operator.and_, "Or": operator.or_}
_PREFIX = "__rp_field_"
_FIELD = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)")


@dataclass(frozen=True)
class ExpressionSpec:
    """lookback 为输出当天之前至少需要加载的交易会话数。"""

    expression: str
    fields: tuple[str, ...]
    lookback: int
    operators: tuple[str, ...]

    def to_dict(self) -> dict:
        return {"expression": self.expression, "fields": list(self.fields),
                "lookback": self.lookback, "operators": list(self.operators)}


def _parse(expression: str) -> ast.AST:
    if not isinstance(expression, str) or not expression.strip():
        raise QlibExpressionError("表达式必须为非空字符串")
    if _PREFIX in expression:
        raise QlibExpressionError("字段必须使用 $字段名")
    try:
        return ast.parse(_FIELD.sub(lambda match: _PREFIX + match.group(1), expression.strip()), mode="eval").body
    except (SyntaxError, ValueError) as exc:
        raise QlibExpressionError("表达式语法无效") from exc


def validate_expression(expression: str, fields: Sequence[str], max_window: int = 252) -> ExpressionSpec:
    """只检查声明，不读取数据、不导入 Qlib，也不计算因子。"""
    if isinstance(max_window, bool) or not isinstance(max_window, int) or max_window < 1:
        raise QlibExpressionError("max_window 必须为正整数")
    if isinstance(fields, str) or not fields or any(not isinstance(field, str) for field in fields):
        raise QlibExpressionError("fields 必须为非空字段名序列")
    available = set(fields)
    used: set[str] = set()
    ops: set[str] = set()

    def window(node: ast.AST) -> int:
        if not isinstance(node, ast.Constant) or type(node.value) is not int or not 1 <= node.value <= max_window:
            raise QlibExpressionError(f"窗口和 Ref 位移只允许 1 到 {max_window} 的整数；未来引用、Ref(0) 和无限窗不受支持")
        return node.value

    def walk(node: ast.AST) -> tuple[int, bool]:
        if isinstance(node, ast.Constant):
            if type(node.value) not in (int, float) or not math.isfinite(node.value):
                raise QlibExpressionError("常量必须为有限实数")
            return 0, False
        if isinstance(node, ast.Name) and node.id.startswith(_PREFIX):
            field = node.id[len(_PREFIX):]
            if field not in available:
                raise QlibExpressionError(f"未声明的字段: {field}")
            used.add(field)
            return 0, True
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub)):
            result = walk(node.operand)
            if isinstance(node.op, ast.USub):
                ops.add("Mul")
            return result
        if isinstance(node, ast.BinOp) and type(node.op) in _BINARY:
            ops.add(_BINARY[type(node.op)])
            left, right = walk(node.left), walk(node.right)
            return max(left[0], right[0]), left[1] or right[1]
        if isinstance(node, ast.Compare) and len(node.ops) == 1 and type(node.ops[0]) in _COMPARE:
            ops.add(_COMPARE[type(node.ops[0])])
            left, right = walk(node.left), walk(node.comparators[0])
            return max(left[0], right[0]), left[1] or right[1]
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name) or node.keywords:
            raise QlibExpressionError("仅允许字段、数值、算术、单次比较和已声明的 Qlib 算子调用")
        name = node.func.id
        arity = (1 if name in _UNARY else 2 if name in _PAIR | _ROLLING | _SHIFT
                 else 3 if name in _PAIR_ROLLING | {"Quantile", "If"} else None)
        if arity is None or len(node.args) != arity:
            raise QlibExpressionError(f"未知算子或参数数量错误: {name}")
        ops.add(name)
        if name in _ROLLING | _SHIFT | {"Quantile"}:
            lookback, depends = walk(node.args[0])
            if not depends:
                raise QlibExpressionError(f"{name} 的输入必须引用数据字段")
            count = window(node.args[1])
            if name == "Quantile":
                quantile = node.args[2]
                if not isinstance(quantile, ast.Constant) or type(quantile.value) not in (int, float) or not 0 <= quantile.value <= 1:
                    raise QlibExpressionError("Quantile 分位数必须为 [0, 1] 内的数值常量")
            return lookback + (count if name in _SHIFT else count - 1), True
        if name in _PAIR_ROLLING:
            left, right = walk(node.args[0]), walk(node.args[1])
            if not left[1] or not right[1]:
                raise QlibExpressionError(f"{name} 两个输入都必须引用数据字段")
            return max(left[0], right[0]) + window(node.args[2]) - 1, True
        children = [walk(arg) for arg in node.args]
        if not any(dependent for _, dependent in children):
            raise QlibExpressionError(f"{name} 必须引用数据字段")
        return max(back for back, _ in children), True

    lookback, dependent = walk(_parse(expression))
    if not dependent:
        raise QlibExpressionError("特征表达式至少需要一个数据字段")
    if lookback > max_window:
        raise QlibExpressionError(f"嵌套表达式需要 {lookback} 个历史会话，超过 max_window={max_window}")
    return ExpressionSpec(expression.strip(), tuple(sorted(used)), lookback, tuple(sorted(ops)))


def baseline_expressions(name: str, *, selected: Sequence[str] | None = None,
                         fields: Sequence[str] = ("open", "high", "low", "close", "vwap", "volume"),
                         max_window: int = 252) -> dict[str, str]:
    """读取 Qlib 原生特征定义；不创建标签、处理器或数据加载器。"""
    if name not in {"Alpha158", "Alpha360"}:
        raise QlibExpressionError("基线只支持 Alpha158 或 Alpha360")
    try:
        from qlib.contrib.data.loader import Alpha158DL, Alpha360DL
    except ImportError as exc:
        raise QlibExpressionError("表达式基线需要安装 quantwitness[ml]") from exc
    definitions, names = {"Alpha158": Alpha158DL, "Alpha360": Alpha360DL}[name].get_feature_config()
    result = dict(zip(names, definitions))
    if selected is not None:
        if isinstance(selected, str) or not selected or len(set(selected)) != len(selected):
            raise QlibExpressionError("selected 必须为非空且不重复的基线特征名序列")
        unknown = set(selected) - result.keys()
        if unknown:
            raise QlibExpressionError(f"基线不存在的特征: {sorted(unknown)}")
        result = {key: result[key] for key in selected}
    for expression in result.values():
        validate_expression(expression, fields, max_window=max_window)
    return result


def _local_engine():
    try:
        from qlib.data.base import Expression
        from qlib.data import ops
    except ImportError as exc:
        raise QlibExpressionError("因子数值计算需要安装 quantwitness[ml]") from exc

    class LocalLoad:
        """绕过 Qlib 全局 H 缓存，仅共享本表达式树的当前输入。"""

        def load(self, instrument, start_index, end_index, *args):
            key = (instrument, start_index, end_index, args)
            cache = getattr(self, "_local_cache", None)
            if cache is None:
                cache = self._local_cache = {}
            if key not in cache:
                values = self._load_internal(instrument, start_index, end_index, *args)
                if getattr(self, "_full_window", False):
                    sources = ([self.feature_left, self.feature_right] if isinstance(self, ops.PairRolling) else [self.feature])
                    complete = pd.Series(True, index=values.index)
                    for source in sources:
                        count = ops.Count(source, self.N)._load_internal(instrument, start_index, end_index, *args)
                        complete &= count.eq(self.N)
                    values = values.where(complete)
                cache[key] = values
            return cache[key]

    class FrameField(LocalLoad, Expression):
        def __init__(self, name: str, series: pd.Series):
            self.field_name, self.series = name, series

        def __str__(self):
            return "$" + self.field_name

        def _load_internal(self, instrument, start_index, end_index, *args):
            return self.series.loc[start_index:end_index]

        def get_longest_back_rolling(self):
            return 0

        def get_extended_window_size(self):
            return 0, 0

    classes = {}

    def make(name, *args, full_window=False):
        if name not in classes:
            classes[name] = type(name, (LocalLoad, getattr(ops, name)), {})
        result = classes[name](*args)
        result._full_window = full_window and name in _ROLLING | _PAIR_ROLLING | {"Quantile"}
        return result

    def build(node, leaves, full_window):
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, ast.Name):
            return leaves[node.id[len(_PREFIX):]]
        if isinstance(node, ast.UnaryOp):
            value = build(node.operand, leaves, full_window)
            if isinstance(node.op, ast.UAdd):
                return value
            return make("Mul", value, -1) if isinstance(value, Expression) else -value
        if isinstance(node, ast.BinOp):
            left, right = build(node.left, leaves, full_window), build(node.right, leaves, full_window)
            name = _BINARY[type(node.op)]
            if not isinstance(left, Expression) and not isinstance(right, Expression):
                try:
                    value = _SCALAR_BINARY[name](left, right)
                    if not isinstance(value, (int, float)) or not math.isfinite(value):
                        raise ValueError("非有限实数")
                    return value
                except (ArithmeticError, TypeError, ValueError) as exc:
                    raise QlibExpressionError("表达式常量运算无效") from exc
            return make(name, left, right)
        if isinstance(node, ast.Compare):
            return make(_COMPARE[type(node.ops[0])], build(node.left, leaves, full_window), build(node.comparators[0], leaves, full_window))
        return make(node.func.id, *(build(arg, leaves, full_window) for arg in node.args), full_window=full_window)

    return FrameField, build


def evaluate_expressions(frame: pd.DataFrame, expressions: Mapping[str, str], *, fields: Sequence[str],
                         max_window: int = 252, min_periods: str = "full",
                         sessions: Sequence | None = None, output_start=None, output_end=None) -> pd.DataFrame:
    """按共同会话日历分证券计算，最后裁切输出；保留输入索引与顺序。"""
    if min_periods not in {"full", "qlib"}:
        raise QlibExpressionError("min_periods 必须显式选择 full 或 qlib")
    if not isinstance(expressions, Mapping) or not expressions or any(not isinstance(name, str) or not name for name in expressions):
        raise QlibExpressionError("expressions 必须为非空特征名到表达式的映射")
    specs = {name: validate_expression(expression, fields, max_window) for name, expression in expressions.items()}
    if not isinstance(frame.index, pd.MultiIndex) or list(frame.index.names) != ["datetime", "instrument"]:
        raise QlibExpressionError("输入必须使用 datetime/instrument 双层索引")
    if frame.empty or frame.index.has_duplicates or not frame.columns.is_unique:
        raise QlibExpressionError("输入必须非空，日期证券和字段名均不能重复")
    dates = frame.index.get_level_values("datetime")
    instruments = frame.index.get_level_values("instrument")
    if not isinstance(dates, pd.DatetimeIndex) or dates.hasnans or not (dates == dates.normalize()).all():
        raise QlibExpressionError("datetime 必须是无缺失的日频日期索引")
    if instruments.hasnans or any(not isinstance(item, str) or not item for item in instruments):
        raise QlibExpressionError("instrument 必须为非空证券字符串")
    needed = sorted({field for spec in specs.values() for field in spec.fields})
    if not set(needed) <= set(frame.columns):
        raise QlibExpressionError(f"输入缺少字段: {sorted(set(needed) - set(frame.columns))}")
    try:
        data = frame.loc[:, needed].astype(float)
    except (TypeError, ValueError) as exc:
        raise QlibExpressionError("表达式输入字段必须为数值") from exc
    if np.isinf(data.to_numpy()).any():
        raise QlibExpressionError("表达式输入不允许无穷值")
    calendar = dates.unique().sort_values() if sessions is None else pd.DatetimeIndex(sessions)
    if calendar.has_duplicates or calendar.hasnans or not calendar.is_monotonic_increasing or not (calendar == calendar.normalize()).all():
        raise QlibExpressionError("sessions 必须是严格递增且无缺失的会话日期")
    if not dates.isin(calendar).all():
        raise QlibExpressionError("输入日期必须全部位于声明的 sessions 内")
    field_class, build = _local_engine()
    result = pd.DataFrame(np.nan, index=frame.index, columns=list(expressions), dtype=float)
    nodes = {name: _parse(spec.expression) for name, spec in specs.items()}
    for instrument in instruments.unique():
        group = data.xs(instrument, level="instrument").reindex(calendar)
        leaves = {field: field_class(field, pd.Series(group[field].to_numpy(), index=pd.RangeIndex(len(calendar)))) for field in needed}
        positions = np.flatnonzero(instruments == instrument)
        calendar_positions = calendar.get_indexer(dates[positions])
        for name, node in nodes.items():
            expression = build(node, leaves, min_periods == "full")
            with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
                values = expression.load(instrument, 0, len(calendar) - 1).astype(float)
            if min_periods == "full":
                values = values.copy()
                values.iloc[:specs[name].lookback] = np.nan
            if np.isinf(values.to_numpy()).any():
                raise QlibExpressionError(f"特征 {name} 产生无穷值；需明确零分母处理")
            result.iloc[positions, result.columns.get_loc(name)] = values.iloc[calendar_positions].to_numpy()
    mask = np.ones(len(result), dtype=bool)
    if output_start is not None:
        mask &= dates >= pd.Timestamp(output_start)
    if output_end is not None:
        mask &= dates <= pd.Timestamp(output_end)
    result = result.loc[mask]
    result.attrs["qlib_expressions"] = {"min_periods": min_periods, "max_window": max_window,
        "calendar_source": "explicit" if sessions is not None else "input_union",
        "features": {name: spec.to_dict() for name, spec in specs.items()}}
    return result
