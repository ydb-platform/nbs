UNITTEST_FOR(contrib/ydb/core/tx/datashard)

FORK_SUBTESTS()

SPLIT_FACTOR(1)

REQUIREMENTS(cpu:2)
IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    contrib/ydb/core/tx/datashard/ut_common
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/public/lib/yson_value
    contrib/ydb/public/sdk/cpp/src/client/result
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_filter_kmeans.cpp
    ut_fulltext.cpp
    ut_fulltext_dict.cpp
    ut_helpers.cpp
    ut_local_kmeans.cpp
    ut_prefix_kmeans.cpp
    ut_recompute_kmeans.cpp
    ut_reshuffle_kmeans.cpp
    ut_sample_k.cpp
    ut_secondary_index.cpp
    ut_unique_index.cpp
)

END()
