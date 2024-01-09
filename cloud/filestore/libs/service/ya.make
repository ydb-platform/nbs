LIBRARY()

SRCS(
    auth_provider.cpp
    auth_scheme.cpp
    context.cpp
    endpoint.cpp
    endpoint_test.cpp
    error.cpp
    filestore.cpp
    filestore_test.cpp
    request.cpp
    service_auth.cpp
)

PEERDIR(
    cloud/filestore/config
    cloud/filestore/public/api/protos

    cloud/storage/core/libs/common
    cloud/storage/core/protos

    library/cpp/protobuf/util
    library/cpp/threading/future
)

END()
