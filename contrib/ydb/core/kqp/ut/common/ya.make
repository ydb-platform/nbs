LIBRARY()

SRCS(
    json2_udf.cpp
    kqp_ut_common.cpp
    kqp_ut_common.h
    re2_udf.cpp
    string_udf.cpp
    columnshard.cpp
    datetime2_udf.cpp
    math_udf.cpp
    unicode_udf.cpp
    digest_udf.cpp
)

PEERDIR(
    library/cpp/testing/common
    contrib/ydb/core/kqp/federated_query
    contrib/ydb/core/testlib
    contrib/ydb/library/yql/providers/s3/actors_factory
    yql/essentials/public/udf
    yql/essentials/udfs/common/string
    yql/essentials/udfs/common/math
    yql/essentials/udfs/common/unicode_base
    yql/essentials/utils/backtrace
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

END()
