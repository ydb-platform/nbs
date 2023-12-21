LIBRARY()

SRCS(
    tablet_proxy.cpp
    tablet_proxy_actor.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/api
    cloud/filestore/libs/storage/core
    ydb/core/tablet
)

END()

RECURSE_FOR_TESTS(
    ut
)
