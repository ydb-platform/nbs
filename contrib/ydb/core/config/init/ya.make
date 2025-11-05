LIBRARY()

SRCS(
    init.h
    init.cpp
    init_noop.cpp
    dummy.h
    dummy.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/driver_lib/cli_base
    contrib/ydb/core/driver_lib/cli_config_base
    contrib/ydb/core/protos
    contrib/ydb/library/yaml_config
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/public/udf
    contrib/ydb/public/lib/deprecated/kicli
    contrib/ydb/public/sdk/cpp/client/ydb_discovery
    contrib/ydb/public/sdk/cpp/client/ydb_driver
)

GENERATE_ENUM_SERIALIZATION(init.h)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)

