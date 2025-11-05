LIBRARY()

SRCS(
    fake_quota_manager.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/core/fq/libs/quota_manager/events
)

END()
