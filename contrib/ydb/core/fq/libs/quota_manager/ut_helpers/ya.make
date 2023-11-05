LIBRARY()

SRCS(
    fake_quota_manager.cpp
)

PEERDIR(
    library/cpp/actors/core
    contrib/ydb/core/fq/libs/quota_manager/events
)

END()
