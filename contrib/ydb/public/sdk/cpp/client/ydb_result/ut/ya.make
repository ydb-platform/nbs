UNITTEST_FOR(contrib/ydb/public/sdk/cpp/client/ydb_result)

IF (SANITIZER_TYPE == "thread")
    TIMEOUT(1200)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

SRCS(
    result_ut.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/client/ydb_params
)

END()
