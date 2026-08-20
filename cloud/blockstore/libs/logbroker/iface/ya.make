LIBRARY()

SRCS(
    config.cpp
    credentials_provider.cpp
    logbroker.cpp
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics

    library/cpp/monlib/service/pages
    library/cpp/threading/future

    contrib/libs/ydb-cpp-sdk/src/client/iam
)

END()

RECURSE_FOR_TESTS(ut)
