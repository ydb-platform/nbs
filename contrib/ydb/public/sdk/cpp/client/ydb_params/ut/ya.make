UNITTEST_FOR(contrib/ydb/public/sdk/cpp/client/ydb_params)

IF (SANITIZER_TYPE == "thread")
    TIMEOUT(1200)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

PEERDIR(
    contrib/ydb/public/lib/yson_value
)

SRCS(
    params_ut.cpp
)

END()
