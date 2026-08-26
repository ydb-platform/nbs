UNITTEST_FOR(contrib/ydb/library/yaml_config)

PEERDIR(
    contrib/ydb/library/yaml_config/ut/protos
)

SRCS(
    console_dumper_ut.cpp
    yaml_config_helpers_ut.cpp
    yaml_config_ut.cpp
    yaml_config_parser_ut.cpp
    yaml_config_proto2yaml_ut.cpp
)

END()
