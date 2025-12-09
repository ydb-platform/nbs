LIBRARY()

SRCS(
    ydb_yson_value.cpp
)

PEERDIR(
    library/cpp/yson
    library/cpp/yson/node
    contrib/ydb/public/sdk/cpp/client/ydb_result
    contrib/ydb/public/sdk/cpp/client/ydb_value
    yql/essentials/types/uuid
)

END()
