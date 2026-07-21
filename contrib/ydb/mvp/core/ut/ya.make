UNITTEST_FOR(contrib/ydb/mvp/core)

SIZE(SMALL)

SRCS(
    cracked_page_ut.cpp
    mvp_security_printer_ut.cpp
    mvp_startup_options_migration_ut.cpp
    mvp_startup_options_ut.cpp
    mvp_startup_options_validation_ut.cpp
    mvp_test_runtime.cpp
    mvp_tokens.cpp
    mvp_ut.cpp
)

PEERDIR(
    contrib/ydb/core/testlib/actors
    contrib/libs/jwt-cpp
    contrib/ydb/library/testlib/service_mocks
    contrib/ydb/mvp/core/ut/protos
)

END()
