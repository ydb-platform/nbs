UNITTEST_FOR(contrib/ydb/public/sdk/cpp/client/extensions/discovery_mutator)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

PEERDIR(
    contrib/ydb/public/sdk/cpp/client/ydb_table
)

SRCS(
    discovery_mutator_ut.cpp
)

END()
