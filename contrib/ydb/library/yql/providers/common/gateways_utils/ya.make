LIBRARY()

    SRCS(
        gateways_utils.cpp
    )

    PEERDIR(
        contrib/ydb/library/yql/utils
        contrib/ydb/library/yql/providers/common/proto
        contrib/ydb/library/yql/providers/common/provider
    )

END()
