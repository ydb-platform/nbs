PY3TEST()

TEST_SRCS(
    test_transform.py
)

ENV(DUMP_BINARY="ydb/library/yaml_config/tools/dump/yaml-to-proto-dump")
ENV(DUMP_DS_INIT_BINARY="ydb/library/yaml_config/tools/dump_ds_init/yaml-to-proto-dump-ds-init")
ENV(JSON_DIFF_BINARY="ydb/library/yaml_config/tools/simple_json_diff/simple_json_diff")

DEPENDS(
    contrib/ydb/library/yaml_config/tools/dump
    contrib/ydb/library/yaml_config/tools/dump_ds_init
    contrib/ydb/library/yaml_config/tools/simple_json_diff
)

DATA(
    arcadia/contrib/ydb/library/yaml_config/ut_transform/configs
    arcadia/contrib/ydb/library/yaml_config/ut_transform/simplified_configs
)

PEERDIR(
    contrib/python/pytest
    contrib/ydb/public/sdk/python/enable_v3_new_behavior
    contrib/ydb/tests/library
    contrib/ydb/tests/oss/canonical
)

SIZE(MEDIUM)

END()
