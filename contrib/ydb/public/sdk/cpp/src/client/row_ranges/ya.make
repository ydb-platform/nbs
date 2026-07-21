LIBRARY()

SRCS(
    rows.cpp
    rows_stream_drain.h
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/query
    contrib/ydb/public/sdk/cpp/src/client/result
    contrib/ydb/public/sdk/cpp/src/client/table
)

END()
