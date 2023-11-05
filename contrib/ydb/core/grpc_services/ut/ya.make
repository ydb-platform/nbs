UNITTEST_FOR(contrib/ydb/core/grpc_services)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    rpc_calls_ut.cpp
    operation_helpers_ut.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    contrib/ydb/core/client/scheme_cache_lib
    contrib/ydb/core/testlib/default
)

END()
