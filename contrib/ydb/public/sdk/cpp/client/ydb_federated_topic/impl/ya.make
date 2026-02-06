LIBRARY()

SRCS(
    federated_read_session.h
    federated_read_session.cpp
    federated_read_session_event.cpp
    federated_write_session.h
    federated_write_session.cpp
    federated_topic_impl.h
    federated_topic_impl.cpp
    federated_topic.cpp
    federation_observer.h
    federation_observer.cpp
)

PEERDIR(
    contrib/ydb/library/grpc/client
    library/cpp/monlib/dynamic_counters
    library/cpp/monlib/metrics
    library/cpp/string_utils/url
    contrib/ydb/library/persqueue/obfuscate
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/api/grpc
    contrib/ydb/public/sdk/cpp/client/impl/ydb_internal/make_request
    contrib/ydb/public/sdk/cpp/client/ydb_common_client/impl
    contrib/ydb/public/sdk/cpp/client/ydb_driver
    contrib/ydb/public/sdk/cpp/client/ydb_topic/impl
    contrib/ydb/public/sdk/cpp/client/ydb_proto
)

END()
