UNITTEST_FOR(contrib/ydb/core/driver_lib/run)

FORK_SUBTESTS()

SIZE(SMALL)

PEERDIR(
    contrib/ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    columnshard_services_ut.cpp
    local_services_ut.cpp
    auto_config_initializer_ut.cpp
    config_helpers_ut.cpp
)

END()
