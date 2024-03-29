LIBRARY()

SRCS(
    infer_schema.cpp
)

PEERDIR(
    library/cpp/yson/node
    yt/cpp/mapreduce/interface
    contrib/ydb/library/yql/public/issue
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/core/issue
)

IF(LINUX)
    PEERDIR(
        yt/yt/client
        contrib/ydb/library/yql/providers/yt/lib/yt_rpc_helpers
    )

    SRCS(
        infer_schema_rpc.cpp
    )
ELSE()
    SRCS(
        infer_schema_rpc_impl.cpp
    )
ENDIF()

END()
