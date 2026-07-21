UNITTEST()

FORK_SUBTESTS()

PEERDIR(
    contrib/ydb/core/tx/datashard/ut_common
    contrib/ydb/core/tx/datashard
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/public/lib/yson_value
    contrib/ydb/public/sdk/cpp/src/client/result
    contrib/ydb/core/tx/schemeshard/ut_helpers
)

YQL_LAST_ABI_VERSION()

SRCS(
    datashard_ut_truncate.cpp
)

END()
