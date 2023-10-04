LIBRARY()

SRCS(
    context.cpp
    endpoint.cpp
    endpoint_test.cpp
    error.cpp
    filestore.cpp
    filestore_test.cpp
    request.cpp
)

PEERDIR(
    cloud/filestore/public/api/protos

    cloud/storage/core/libs/common

    library/cpp/protobuf/util
    library/cpp/threading/future
)

END()
