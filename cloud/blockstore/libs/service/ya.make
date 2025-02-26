LIBRARY()

SRCS(
    aligned_device_handler.cpp
    auth_provider.cpp
    auth_scheme.cpp
    checksum_storage_wrapper.cpp
    context.cpp
    device_handler.cpp
    rdma_protocol.cpp
    rdma_target.cpp
    request_helpers.cpp
    request.cpp
    service_auth.cpp
    service_error_transform.cpp
    service_filtered.cpp
    service_null.cpp
    service_test.cpp
    service.cpp
    storage_provider.cpp
    storage_test.cpp
    storage.cpp
    unaligned_device_handler.cpp
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/libs/common
    cloud/blockstore/public/api/protos

    cloud/blockstore/libs/storage/protos

    cloud/storage/core/libs/common

    library/cpp/lwtrace
)

END()

RECURSE_FOR_TESTS(ut)
