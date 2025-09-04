LIBRARY()

CFLAGS(
    -Wno-deprecated-declarations
)

SRCS(
    yql_s3_datasink.cpp
    yql_s3_datasink_execution.cpp
    yql_s3_datasink_type_ann.cpp
    yql_s3_datasource.cpp
    yql_s3_datasource_type_ann.cpp
    yql_s3_dq_integration.cpp
    yql_s3_exec.cpp
    yql_s3_io_discovery.cpp
    yql_s3_listing_strategy.cpp
    yql_s3_logical_opt.cpp
    yql_s3_mkql_compiler.cpp
    yql_s3_phy_opt.cpp
    yql_s3_provider.cpp
    yql_s3_provider_impl.cpp
    yql_s3_settings.cpp
)

PEERDIR(
    contrib/libs/re2
    library/cpp/json
    library/cpp/protobuf/util
    library/cpp/random_provider
    library/cpp/retry
    library/cpp/time_provider
    library/cpp/xml/document
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/core/type_ann
    contrib/ydb/library/yql/dq/expr_nodes
    contrib/ydb/library/yql/dq/integration
    contrib/ydb/library/yql/minikql/comp_nodes
    contrib/ydb/library/yql/providers/common/config
    contrib/ydb/library/yql/providers/common/dq
    contrib/ydb/library/yql/providers/common/http_gateway
    contrib/ydb/library/yql/providers/common/mkql
    contrib/ydb/library/yql/providers/common/proto
    contrib/ydb/library/yql/providers/common/provider
    contrib/ydb/library/yql/providers/common/schema/expr
    contrib/ydb/library/yql/providers/common/structured_token
    contrib/ydb/library/yql/providers/common/token_accessor/client
    contrib/ydb/library/yql/providers/common/transform
    contrib/ydb/library/yql/providers/dq/common
    contrib/ydb/library/yql/providers/dq/expr_nodes
    contrib/ydb/library/yql/providers/result/expr_nodes
    contrib/ydb/library/yql/providers/s3/common
    contrib/ydb/library/yql/providers/s3/expr_nodes
    contrib/ydb/library/yql/providers/s3/object_listers
    contrib/ydb/library/yql/providers/s3/path_generator
    contrib/ydb/library/yql/providers/s3/proto
    contrib/ydb/library/yql/providers/s3/range_helpers
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/utils/threading
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
