LIBRARY()

SRCS(
    common.cpp
)

PEERDIR(
    library/cpp/monlib/dynamic_counters

    contrib/ydb/library/conclusion
    contrib/ydb/library/yql/dq/actors/protos

    contrib/ydb/library/yql/public/issue
)

YQL_LAST_ABI_VERSION()

END()
