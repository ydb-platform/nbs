LIBRARY()

SRCS(
    app.cpp
    loadtest.cpp
    options.cpp
)

PEERDIR(
    cloud/fastshard/bootstrap
    cloud/fastshard/protos
    cloud/fastshard/sn/client
    cloud/fastshard/sn/iface
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
