LIBRARY()

IF (OPENSOURCE AND NOT FORCE_FASTSHARD_IPC_STUB)
    SRCS(
        storage_node.cpp
    )

    PEERDIR(
        contrib/libs/silk/src/fibers
    )
ELSE()
    SRCS(
        storage_node_stub.cpp
    )
ENDIF()

PEERDIR(
    cloud/filestore/libs/storage/fastshard/sn/iface

    cloud/storage/core/libs/common
)

END()

IF (OPENSOURCE AND NOT FORCE_FASTSHARD_IPC_STUB)
    RECURSE_FOR_TESTS(
        ut
    )
ENDIF()
