LIBRARY()

SRCS(
    service_statistics_collector_actor.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/api
    contrib/ydb/library/actors/core
)

END()

RECURSE()
