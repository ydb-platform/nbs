UNITTEST()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

SRCS(
   client_session_ut.cpp
   deferred_session_creation_ut.cpp
   query_stats_ut.cpp
)

PEERDIR(
    library/cpp/testing/common
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/src/library/operation_id
    contrib/ydb/public/sdk/cpp/src/client/impl/session
    contrib/ydb/public/sdk/cpp/src/client/query/impl 
    contrib/ydb/public/sdk/cpp/src/client/query
)

END()
