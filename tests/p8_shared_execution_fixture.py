"""历史期货结果表的读取夹具。"""

def rows(result, name):
    table = result.tables[name]
    return table.to_dict('records') if hasattr(table, 'to_dict') else table.to_pylist()


