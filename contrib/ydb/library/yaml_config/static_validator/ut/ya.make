UNITTEST()

SRCS(
    test.cpp
)

PEERDIR(
    contrib/ydb/library/yaml_config/static_validator
    contrib/ydb/library/yaml_config/validator
)

END()

RECURSE_FOR_TESTS(example_configs)
