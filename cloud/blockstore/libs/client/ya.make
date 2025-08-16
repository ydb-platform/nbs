LIBRARY()

SRCS(
    client.cpp
    config.cpp
    durable.cpp
    metric.cpp
    multiclient_endpoint.cpp
    session.cpp
    session_test.cpp
    throttling.cpp
)

PEERDIR(
    cloud/blockstore/public/api/grpc
    cloud/blockstore/public/api/protos

    cloud/blockstore/config
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/service
    cloud/blockstore/libs/throttling
    cloud/storage/core/libs/grpc
    cloud/storage/core/libs/throttling

    library/cpp/lwtrace
    library/cpp/monlib/dynamic_counters
    library/cpp/threading/future
    library/cpp/monlib/service
    library/cpp/monlib/service/pages

    contrib/libs/grpc
)

IF (SANITIZER_TYPE == "thread")
    SUPPRESSIONS(
        tsan.supp
    )
ENDIF()

END()

# Until DEVTOOLSSUPPORT-25698 is not solved.
IF (SANITIZER_TYPE == "address" OR SANITIZER_TYPE == "memory")
    RECURSE_FOR_TESTS(
        ut
    )
ELSE()
    RECURSE_FOR_TESTS(
        ut
        ut_throttling
    )
ENDIF()
