YQL_UDF_CONTRIB(datetime2_udf)

YQL_ABI_VERSION(
    2
    35
    0
)

SRCS(
    datetime_udf.cpp
)

PEERDIR(
    util/draft
    contrib/ydb/library/yql/public/udf/arrow
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/minikql/datetime
    contrib/ydb/library/yql/public/udf/tz
)

END()

RECURSE_FOR_TESTS(
    test
)
