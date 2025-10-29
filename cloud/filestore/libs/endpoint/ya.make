LIBRARY()

SRCS(
    endpoint_manager.cpp
    listener.cpp
    service_auth.cpp
)

PEERDIR(
    cloud/filestore/libs/service

    cloud/storage/core/libs/common
    cloud/storage/core/libs/coroutine
    cloud/storage/core/libs/diagnostics

    ydb/core/protos
)

END()

RECURSE_FOR_TESTS(ut)
