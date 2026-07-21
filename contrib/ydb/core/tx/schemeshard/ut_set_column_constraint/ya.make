UNITTEST_FOR(contrib/ydb/core/tx/schemeshard)

FORK_SUBTESTS()

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    contrib/ydb/core/grpc_services/local_rpc
    contrib/ydb/core/metering
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx
    contrib/ydb/core/tx/schemeshard/ut_helpers
    contrib/ydb/public/api/protos
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_set_column_constraint.cpp
)

END()
