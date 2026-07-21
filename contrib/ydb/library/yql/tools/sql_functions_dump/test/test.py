import yatest.common
import json
import os

DATA_PATH = yatest.common.source_path('contrib/ydb/library/yql/data/language')
TOOL_PATH = yatest.common.binary_path('contrib/ydb/library/yql/tools/sql_functions_dump/sql_functions_dump')


def test_functions_dump():
    with open(os.path.join(DATA_PATH, "sql_functions.json")) as f:
        func_from_file = json.load(f)
    res = yatest.common.execute([TOOL_PATH], check_exit_code=True, wait=True)
    func_from_tool = json.loads(res.stdout)
    assert func_from_tool == func_from_file, (
        'JSON_DIFFER\n' 'File:\n %(func_from_file)s\n\n' 'Tool:\n %(func_from_tool)s\n' % locals()
    )
