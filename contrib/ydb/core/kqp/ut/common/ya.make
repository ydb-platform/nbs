LIBRARY()

SRCS(
    arrow_builders.cpp
    kqp_ut_common.cpp
    kqp_ut_common.h
    columnshard.cpp
)

PEERDIR(
    library/cpp/testing/common
    contrib/ydb/core/kqp/federated_query
    contrib/ydb/core/testlib
    contrib/ydb/library/testlib/common
    contrib/ydb/library/yql/providers/s3/actors_factory
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/udfs/common/datetime2
    contrib/ydb/library/yql/udfs/common/digest
    contrib/ydb/library/yql/udfs/common/json2
    contrib/ydb/library/yql/udfs/common/math
    contrib/ydb/library/yql/udfs/common/re2
    contrib/ydb/library/yql/udfs/common/string
    contrib/ydb/library/yql/udfs/common/unicode_base
    contrib/ydb/library/yql/utils/backtrace
    contrib/ydb/public/lib/yson_value
    contrib/ydb/core/tx/columnshard/test_helper
    contrib/ydb/public/sdk/cpp/src/client/draft
    contrib/ydb/public/sdk/cpp/src/client/query
    contrib/ydb/public/sdk/cpp/src/client/proto
    contrib/ydb/public/sdk/cpp/src/client/scheme
    contrib/ydb/public/sdk/cpp/src/client/table
    contrib/ydb/public/sdk/cpp/src/client/topic
    contrib/libs/highwayhash
)

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(olap_indexes_enums.h)

END()
