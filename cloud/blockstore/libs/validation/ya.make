LIBRARY()

SRCS(
    validation.cpp
    validation_client.cpp
    data_integrity_client.cpp
    validation_service.cpp
)

PEERDIR(
    cloud/blockstore/public/api/protos

    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/service

    library/cpp/digest/crc32c
    library/cpp/monlib/dynamic_counters
    library/cpp/monlib/service
    library/cpp/monlib/service/pages
    library/cpp/threading/future

    contrib/libs/sparsehash
)

END()

RECURSE_FOR_TESTS(ut)
