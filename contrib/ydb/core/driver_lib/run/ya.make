LIBRARY(run)

ADDINCL(
    contrib/ydb/public/sdk/cpp
)

SRCS(
    columnshard_services.cpp
    full_runner.cpp
    main.cpp
)

PEERDIR(
    contrib/ydb/core/driver_lib/run/common
    contrib/ydb/core/tx/conveyor_composite/service
    contrib/ydb/core/tx/priorities/service
    contrib/ydb/core/tx/columnshard
    contrib/ydb/core/tx/columnshard/data_accessor/cache_policy
    contrib/ydb/core/tx/columnshard/column_fetching
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)

RECURSE_ROOT_RELATIVE(
    contrib/ydb/core
    contrib/ydb/services
)
