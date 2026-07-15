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
    cloud/filestore/libs/storage/fastshard/sn/client
    cloud/filestore/libs/storage/fastshard/sn/quorum
    cloud/filestore/libs/storage/model

    cloud/filestore/private/api/unsafe_protos
)

END()

# TODO(#5895): fix silk bootstrap/shutdown under msan
IF (OPENSOURCE AND NOT FORCE_FASTSHARD_IPC_STUB AND SANITIZER_TYPE != "memory")
    RECURSE_FOR_TESTS(
        ut
    )
ENDIF()
