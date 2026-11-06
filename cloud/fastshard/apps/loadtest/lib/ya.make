LIBRARY()

SRCS(
    app.cpp
    loadtest.cpp
    options.cpp
)

PEERDIR(
    cloud/fastshard/libs/bootstrap
    cloud/fastshard/libs/common
    cloud/fastshard/libs/sn/client
    cloud/fastshard/libs/sn/iface
    cloud/fastshard/public/api/protos
    cloud/filestore/tools/testing/loadtest/protos

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/protos

    library/cpp/getopt
    library/cpp/protobuf/json

    contrib/libs/silk/src/fibers
)

END()

RECURSE_FOR_TESTS(
    ut
)
