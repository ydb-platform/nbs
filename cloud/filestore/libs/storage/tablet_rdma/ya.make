LIBRARY()

SRCS(
    rdma_tablet_registry.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/api

    contrib/ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(ut)
