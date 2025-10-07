LIBRARY()

SRCS(
    config.cpp
    critical_events.cpp
    incomplete_requests.cpp
    profile_log.cpp
    profile_log_events.cpp
    request_stats.cpp
    throttler_info_serializer.cpp
    trace_serializer.cpp
    user_counter.cpp
)

PEERDIR(
    cloud/filestore/config
    cloud/filestore/libs/diagnostics/events
    cloud/filestore/libs/service
    cloud/filestore/libs/storage/core
    # FIXME use public api protos
    cloud/filestore/libs/storage/tablet/protos
    cloud/filestore/private/api/protos
    cloud/filestore/public/api/protos

    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/user_stats
    cloud/storage/core/libs/version

    library/cpp/eventlog
    library/cpp/monlib/dynamic_counters
    library/cpp/monlib/service/pages
    library/cpp/protobuf/util
)

END()

RECURSE(
    metrics
)

RECURSE_FOR_TESTS(
    ut
)
