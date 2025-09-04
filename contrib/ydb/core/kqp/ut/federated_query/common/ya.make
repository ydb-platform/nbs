LIBRARY()

SRCS(
    common.cpp
)

STYLE_CPP()

PEERDIR(
    contrib/ydb/core/kqp/rm_service
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/library/yql/providers/s3/actors_factory
    contrib/ydb/public/sdk/cpp/src/client/operation
    contrib/ydb/public/sdk/cpp/src/client/query
)

YQL_LAST_ABI_VERSION()

END()
