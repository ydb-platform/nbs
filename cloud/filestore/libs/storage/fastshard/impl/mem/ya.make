LIBRARY()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/deny_ydb_dependency.inc)

SRCS(
    memshard.cpp
)

PEERDIR(
    cloud/filestore/libs/service
    cloud/filestore/libs/storage/fastshard/iface

    cloud/filestore/private/api/unsafe_protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
