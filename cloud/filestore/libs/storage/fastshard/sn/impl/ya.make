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

# TODO(#5895): fix silk bootstrap/shutdown under msan
IF (OPENSOURCE AND NOT FORCE_FASTSHARD_IPC_STUB AND SANITIZER_TYPE != "memory")
    RECURSE_FOR_TESTS(
        ut
    )
ENDIF()
