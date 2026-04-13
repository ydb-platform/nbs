UNITTEST_FOR(contrib/ydb/services/dynamic_config)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    dynamic_config_ut.cpp
)

PEERDIR(
    library/cpp/getopt
    contrib/ydb/public/sdk/cpp/src/library/grpc/client
    library/cpp/regex/pcre
    library/cpp/svnversion
    contrib/ydb/core/testlib/default
    contrib/ydb/services/cms
)

YQL_LAST_ABI_VERSION()

END()
