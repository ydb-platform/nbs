UNITTEST_FOR(contrib/ydb/services/persqueue_cluster_discovery)

FORK_SUBTESTS()

IF (WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(ram:32)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    cluster_discovery_service_ut.cpp
)

PEERDIR(
    contrib/ydb/library/actors/http
    contrib/ydb/core/testlib/default
    contrib/ydb/public/api/grpc
    contrib/ydb/services/persqueue_cluster_discovery
)

YQL_LAST_ABI_VERSION()

END()
