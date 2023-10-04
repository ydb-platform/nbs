LIBRARY()

SRCS(
    service.cpp
)

PEERDIR(
    cloud/filestore/libs/service
    cloud/filestore/libs/storage/api

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/kikimr
)

END()

RECURSE_FOR_TESTS(ut)
