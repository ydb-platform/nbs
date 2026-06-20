LIBRARY()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/deny_ydb_dependency.inc)

IF (OPENSOURCE AND NOT FORCE_FASTSHARD_IPC_STUB)
    SRCS(
        shard.cpp
    )

    PEERDIR(
        cloud/filestore/libs/storage/fastshard/ipc

        contrib/libs/silk/src/fibers
    )
ELSE()
    SRCS(
        shard_stub.cpp
    )
ENDIF()

PEERDIR(
    cloud/filestore/libs/service
    cloud/filestore/libs/storage/fastshard/iface

    cloud/filestore/private/api/unsafe_protos
)

END()

IF (OPENSOURCE AND NOT FORCE_FASTSHARD_IPC_STUB)
    RECURSE_FOR_TESTS(
        ut
    )
ENDIF()
