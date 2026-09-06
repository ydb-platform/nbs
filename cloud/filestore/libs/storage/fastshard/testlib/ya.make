LIBRARY()

SRCS(
    fake_storage_node.cpp
)

IF (OPENSOURCE AND NOT FORCE_FASTSHARD_IPC_STUB)
    SRCS(
        silk_env.cpp
    )
ELSE()
    SRCS(
        silk_env_stub.cpp
    )
ENDIF()

PEERDIR(
    cloud/filestore/libs/storage/fastshard/sn/iface

    cloud/storage/core/protos
)

IF (OPENSOURCE AND NOT FORCE_FASTSHARD_IPC_STUB)
    PEERDIR(
        contrib/libs/silk/src/fibers

        contrib/restricted/googletest/googletest
    )
ENDIF()

END()
