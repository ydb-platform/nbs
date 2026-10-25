LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    common.h
    common.cpp
    counters_logger.h
    deferred_commit.cpp
    event_handlers.cpp
    offsets_collector.cpp
    proto_accessor.cpp
    read_session_event.cpp
    read_session_impl.ipp
    read_session.h
    read_session.cpp
    topic_impl.h
    topic_impl.cpp
    topic.cpp
    transaction.cpp
    write_session_impl.h
    write_session_impl.cpp
    write_session.h
    write_session.cpp
)

PEERDIR(
    contrib/libs/ydb-cpp-sdk/src/library/grpc/client
    library/cpp/monlib/dynamic_counters
    library/cpp/monlib/metrics
    library/cpp/string_utils/url
    contrib/libs/ydb-cpp-sdk/src/library/persqueue/obfuscate
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/api/grpc
    contrib/libs/ydb-cpp-sdk/src/client/impl/ydb_internal/make_request
    contrib/libs/ydb-cpp-sdk/src/client/common_client/impl
    contrib/libs/ydb-cpp-sdk/src/client/driver
    contrib/libs/ydb-cpp-sdk/src/client/topic/common
    contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/topic
    contrib/libs/ydb-cpp-sdk/src/client/proto
)

END()
