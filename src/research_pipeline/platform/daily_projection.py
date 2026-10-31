"""日线物理列与规范归档列共用的行情投影。"""


def daily_projection(plan):
    """归档保留规范field_id；原库仍使用准入columns中的物理角色。"""
    roles = {'date', 'code', 'open', 'high', 'low', 'close', 'volume'}
    result = {}
    for logical, physical in plan.columns:
        if logical not in plan.query.field_ids:
            continue
        role = physical
        if logical == physical and logical.startswith(('fld_etf_', 'fld_equity_')):
            role = logical.rsplit('_', 1)[-1]
        if role in roles:
            if role in result and result[role] != logical:
                raise ValueError('日线行情角色存在重复映射：'+role)
            result[role] = logical
    return result
