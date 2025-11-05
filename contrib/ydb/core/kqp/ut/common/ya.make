LIBRARY()

SRCS(
    json2_udf.cpp
    kqp_ut_common.cpp
    kqp_ut_common.h
    re2_udf.cpp
    string_udf.cpp
    columnshard.cpp
    datetime2_udf.cpp
)

PEERDIR(
    library/cpp/testing/common
    contrib/ydb/core/kqp/federated_query
    contrib/ydb/core/testlib
    contrib/ydb/library/yql/providers/s3/actors_factory
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/udfs/common/string
    contrib/ydb/library/yql/utils/backtrace
    contrib/ydb/public/lib/yson_value
    contrib/ydb/core/tx/columnshard/test_helper
    contrib/ydb/public/sdk/cpp/client/draft
    contrib/ydb/public/sdk/cpp/client/ydb_query
    contrib/ydb/public/sdk/cpp/client/ydb_proto
    contrib/ydb/public/sdk/cpp/client/ydb_scheme
    contrib/ydb/public/sdk/cpp/client/ydb_table
    contrib/ydb/public/sdk/cpp/client/ydb_topic
)

YQL_LAST_ABI_VERSION()

END()
