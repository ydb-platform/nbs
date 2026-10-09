LIBRARY()

SRCS(
    delay_policy.cpp
    fake_storage_node.cpp
)

IF (OPENSOURCE AND NOT FORCE_FASTSHARD_IPC_STUB)
    SRCS(
        fiber_test.cpp
        journalled_storage_node.cpp
        silk_env.cpp
    )
ELSE()
    SRCS(
        journalled_storage_node_stub.cpp
        silk_env_stub.cpp
    )
ENDIF()

PEERDIR(
    cloud/fastshard/journal/iface
    cloud/fastshard/protos
    cloud/fastshard/sn/iface

    cloud/storage/core/libs/common
    cloud/storage/core/libs/coroutine
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/protos
)

IF (OPENSOURCE AND NOT FORCE_FASTSHARD_IPC_STUB)
    PEERDIR(
        cloud/fastshard/journal/impl

        contrib/libs/silk/src/fibers
        contrib/restricted/googletest/googletest

        library/cpp/testing/common
        library/cpp/threading/future
    )
ENDIF()

END()
