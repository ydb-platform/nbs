LIBRARY()

SRCS(
    meta_cluster_info.cpp
    meta.cpp
    meta_cache.cpp
    meta_settings.cpp
    meta_versions.cpp
    mvp.cpp
)

PEERDIR(
    contrib/ydb/mvp/core
    contrib/ydb/mvp/meta/support_links
    contrib/ydb/mvp/meta/protos
    contrib/ydb/public/api/client/yc_private/resourcemanager
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/providers/result/expr_nodes
    contrib/ydb/library/yql/core/expr_nodes
    contrib/ydb/library/aclib/protos
    library/cpp/protobuf/json
    library/cpp/getopt
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    bin
    support_links
)

RECURSE_FOR_TESTS(
    ut
)
