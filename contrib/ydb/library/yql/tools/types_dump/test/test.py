import yatest.common
import json
import os

DATA_PATH = yatest.common.source_path('contrib/ydb/library/yql/data/language')
TOOL_PATH = yatest.common.binary_path('contrib/ydb/library/yql/tools/types_dump/types_dump')


def test_types_dump():
    with open(os.path.join(DATA_PATH, "types.json")) as f:
        types_from_file = json.load(f)
    res = yatest.common.execute([TOOL_PATH], check_exit_code=True, wait=True)
    types_from_tool = json.loads(res.stdout)
    assert types_from_tool == types_from_file, (
        'JSON_DIFFER\n' 'File:\n %(types_from_file)s\n\n' 'Tool:\n %(types_from_tool)s\n' % locals()
    )
