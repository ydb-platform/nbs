LIBRARY()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/deny_ydb_dependency.inc)

SRCS(
    fiber_shard.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/fastshard/iface

    cloud/storage/core/libs/common

    contrib/libs/silk/src/fibers
)

END()
