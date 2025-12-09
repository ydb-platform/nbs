UNITTEST_FOR(contrib/ydb/services/fq)

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    ut_utils.cpp
    fq_ut.cpp
)

PEERDIR(
    library/cpp/getopt
    contrib/ydb/public/sdk/cpp/src/library/grpc/client
    library/cpp/regex/pcre
    library/cpp/svnversion
    contrib/ydb/core/fq/libs/control_plane_storage
    contrib/ydb/core/fq/libs/db_id_async_resolver_impl
    contrib/ydb/core/fq/libs/db_schema
    contrib/ydb/core/fq/libs/private_client
    contrib/ydb/core/testlib/default
    contrib/ydb/library/yql/providers/common/db_id_async_resolver
    contrib/ydb/library/yql/udfs/common/clickhouse/client
    yql/essentials/utils/network
    contrib/ydb/public/lib/fq
    contrib/ydb/services/ydb
)

YQL_LAST_ABI_VERSION()

END()
