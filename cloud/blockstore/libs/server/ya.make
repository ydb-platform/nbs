LIBRARY()

SRCS(
    client_storage_factory.cpp
    config.cpp
    server.cpp
    server_test.cpp
)

PEERDIR(
    cloud/blockstore/public/api/grpc
    cloud/blockstore/public/api/protos

    cloud/blockstore/config
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/service
    cloud/storage/core/libs/grpc
    cloud/storage/core/libs/uds

    ydb/library/actors/prof
    library/cpp/monlib/service
    library/cpp/monlib/service/pages

    contrib/libs/grpc
)

IF (SANITIZER_TYPE == "thread")
    SUPPRESSIONS(
        tsan.supp
    )
ENDIF()

IF (PROFILE_MEMORY_ALLOCATIONS)
    CFLAGS(-DPROFILE_MEMORY_ALLOCATIONS)
ENDIF()

END()

RECURSE_FOR_TESTS(ut)
