PROGRAM(blockstore-vhost-server)

SRCS(
    main.cpp

    backend.cpp
    backend_aio.cpp
    backend_rdma.cpp
    backend_null.cpp
    critical_event.cpp
    histogram.cpp
    options.cpp
    request_aio.cpp
    server.cpp
    stats.cpp
)

SPLIT_DWARF()

IF (SANITIZER_TYPE)
    NO_SPLIT_DWARF()
ENDIF()

ADDINCL(
)

PEERDIR(
    cloud/blockstore/libs/client
    cloud/blockstore/libs/common
    cloud/blockstore/libs/encryption
    cloud/blockstore/libs/encryption/model
    cloud/blockstore/libs/rdma/impl
    cloud/blockstore/libs/service_local

    cloud/contrib/vhost

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics

    ydb/library/actors/util
    library/cpp/getopt
    library/cpp/getopt/small
    library/cpp/logger

    contrib/libs/libaio
)

END()

RECURSE_FOR_TESTS(
    gtest
    ut
)
