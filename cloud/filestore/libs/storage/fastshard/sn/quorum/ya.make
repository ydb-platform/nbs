LIBRARY()

IF (OPENSOURCE AND NOT FORCE_FASTSHARD_IPC_STUB)
    SRCS(
        storage_group.cpp
        storage_group_helpers.cpp
        storage_group_quorum.cpp
    )

    PEERDIR(
        contrib/libs/silk/src/fibers
    )
ELSE()
    SRCS(
        storage_group_stub.cpp
    )
ENDIF()

PEERDIR(
    cloud/filestore/libs/storage/fastshard/sn/iface

    cloud/storage/core/libs/common
    cloud/storage/core/protos
)

END()

IF (OPENSOURCE AND NOT FORCE_FASTSHARD_IPC_STUB)
    RECURSE_FOR_TESTS(
        ut
    )
ENDIF()
