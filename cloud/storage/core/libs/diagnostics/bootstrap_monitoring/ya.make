LIBRARY()

SRCS(
    bootstrap_monitoring.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
    contrib/ydb/core/protos
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/http
    contrib/ydb/library/actors/interconnect/poller
    library/cpp/monlib/dynamic_counters
    library/cpp/monlib/service
)

END()

RECURSE_FOR_TESTS(ut)
