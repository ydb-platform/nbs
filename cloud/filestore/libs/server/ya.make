LIBRARY()

SRCS(
    config.cpp
    probes.cpp
    server_memory_state.cpp
    server_stats.cpp
    server.cpp
)

PEERDIR(
    cloud/filestore/config
    cloud/filestore/libs/diagnostics
    cloud/filestore/public/api/grpc
    cloud/filestore/public/api/protos

    cloud/filestore/libs/service

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/grpc
    cloud/storage/core/libs/uds
    cloud/storage/core/protos

    library/cpp/deprecated/atomic
    library/cpp/lwtrace

    contrib/libs/grpc
)

IF (SANITIZER_TYPE == "thread")
    SUPPRESSIONS(
        tsan.supp
    )
ENDIF()

END()

RECURSE_FOR_TESTS(ut)
