LIBRARY(filestore-testing-loadtest-lib)

SRCS(
    client.cpp
    context.h
    executor.cpp
    request_data.cpp
    request_index.cpp
    test.cpp
)

PEERDIR(
    cloud/filestore/config
    cloud/filestore/public/api/protos

    cloud/filestore/libs/client
    cloud/filestore/libs/service

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics

    cloud/filestore/tools/testing/loadtest/protos

    library/cpp/deprecated/atomic
)

END()
