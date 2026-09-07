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

# TODO(#5895): fix silk bootstrap/shutdown under msan
IF (OPENSOURCE AND NOT FORCE_FASTSHARD_IPC_STUB AND SANITIZER_TYPE != "memory")
    RECURSE_FOR_TESTS(
        ut
    )
ENDIF()
