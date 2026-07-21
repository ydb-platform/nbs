UNITTEST_FOR(contrib/ydb/core/grpc_services)

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    rpc_calls_ut.cpp
    rpc_load_rows_ut.cpp
    operation_helpers_ut.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    contrib/ydb/core/client/scheme_cache_lib
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/scheme
    contrib/ydb/core/testlib/default
)

END()
