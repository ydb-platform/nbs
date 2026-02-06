UNITTEST_FOR(contrib/ydb/public/sdk/cpp/client/ydb_result)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
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
