UNITTEST()

PEERDIR(
    contrib/ydb/core/tx/datashard/ut_common
    contrib/ydb/core/tx/datashard
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/testlib/default
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/core/tx/schemeshard/ut_helpers
    contrib/ydb/core/protos
)

YQL_LAST_ABI_VERSION()

SRCS(
    datashard_ut_validate_row_condition_scan.cpp
)

END()
