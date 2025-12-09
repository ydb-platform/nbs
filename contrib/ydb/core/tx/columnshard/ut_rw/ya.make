UNITTEST_FOR(contrib/ydb/core/tx/columnshard)

FORK_SUBTESTS()

SPLIT_FACTOR(60)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(ram:16)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx/columnshard/test_helper
    contrib/ydb/core/tx/columnshard/hooks/abstract
    contrib/ydb/core/tx/columnshard/hooks/testing
    contrib/ydb/services/metadata
    contrib/ydb/core/tx
    contrib/ydb/public/lib/yson_value
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_columnshard_read_write.cpp
    ut_normalizer.cpp
    ut_backup.cpp
)

END()
