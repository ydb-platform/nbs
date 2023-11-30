UNITTEST_FOR(contrib/ydb/services/cms)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    cms_ut.cpp
)

PEERDIR(
    library/cpp/getopt
    contrib/ydb/library/grpc/client
    library/cpp/regex/pcre
    library/cpp/svnversion
    contrib/ydb/core/testlib/default
    contrib/ydb/services/cms
)

YQL_LAST_ABI_VERSION()

END()
