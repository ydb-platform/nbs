LIBRARY(filestore-analytics-libs-event-log)

SRCS(
    dump.cpp
    request_filter.cpp
    request_printer.cpp
)

PEERDIR(
    cloud/filestore/libs/diagnostics
    cloud/filestore/libs/diagnostics/events
    cloud/filestore/libs/service
    cloud/filestore/libs/storage/model
    cloud/filestore/libs/storage/tablet/model

    cloud/storage/core/libs/common
)

END()

RECURSE_FOR_TESTS(
    ut
)
