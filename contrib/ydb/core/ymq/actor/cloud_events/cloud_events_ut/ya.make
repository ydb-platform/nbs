UNITTEST()

PEERDIR(
    contrib/ydb/core/testlib/default
    contrib/ydb/core/ymq/actor/cloud_events
)

SRCS(
    cloud_events_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
