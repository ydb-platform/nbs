LIBRARY()

SRCS(
    actor.cpp
    auth_factory.cpp
    auth_mocks.cpp
    auth_multi_factory.cpp
    attributes_md5.cpp
    cfg.cpp
    change_visibility.cpp
    count_queues.cpp
    cleanup_queue_data.cpp
    create_queue.cpp
    create_user.cpp
    delete_message.cpp
    delete_queue.cpp
    delete_user.cpp
    error.cpp
    executor.cpp
    fifo_cleanup.cpp
    garbage_collector.cpp
    get_queue_attributes.cpp
    get_queue_url.cpp
    index_events_processor.cpp
    infly.cpp
    log.cpp
    list_dead_letter_source_queues.cpp
    list_permissions.cpp
    list_queues.cpp
    list_users.cpp
    local_rate_limiter_allocator.cpp
    message_delay_stats.cpp
    metering.cpp
    modify_permissions.cpp
    monitoring.cpp
    node_tracker.cpp
    proxy_actor.cpp
    purge.cpp
    purge_queue.cpp
    queue_leader.cpp
    receive_message.cpp
    retention.cpp
    schema.cpp
    sha256.cpp
    send_message.cpp
    service.cpp
    set_queue_attributes.cpp
    proxy_service.cpp
    queues_list_reader.cpp
    queue_schema.cpp
    user_settings_names.cpp
    user_settings_reader.cpp
)

PEERDIR(
    contrib/libs/openssl
    contrib/libs/protobuf
    contrib/ydb/library/actors/core
    library/cpp/containers/intrusive_rb_tree
    library/cpp/digest/md5
    contrib/ydb/library/grpc/client
    library/cpp/logger
    library/cpp/lwtrace/mon
    library/cpp/monlib/dynamic_counters
    library/cpp/monlib/service/pages
    library/cpp/scheme
    contrib/ydb/core/base
    contrib/ydb/core/client/minikql_compile
    contrib/ydb/core/engine
    contrib/ydb/core/kesus/tablet
    contrib/ydb/core/mind/address_classification
    contrib/ydb/core/mon
    contrib/ydb/core/node_whiteboard
    contrib/ydb/core/protos
    contrib/ydb/core/tx/scheme_cache
    contrib/ydb/core/tx/schemeshard
    contrib/ydb/core/tx/tx_proxy
    contrib/ydb/core/util
    contrib/ydb/core/ymq/base
    contrib/ydb/core/ymq/proto
    contrib/ydb/core/ymq/queues/common
    contrib/ydb/core/ymq/queues/fifo
    contrib/ydb/core/ymq/queues/std
    contrib/ydb/library/aclib
    contrib/ydb/library/http_proxy/authorization
    contrib/ydb/library/http_proxy/error
    contrib/ydb/library/mkql_proto/protos
    contrib/ydb/public/lib/scheme_types
    contrib/ydb/public/lib/value
    contrib/ydb/public/sdk/cpp/client/ydb_types/credentials
    contrib/ydb/library/yql/minikql
    contrib/ydb/public/lib/deprecated/client
)

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(events.h)

GENERATE_ENUM_SERIALIZATION(metering.h)

GENERATE_ENUM_SERIALIZATION(fifo_cleanup.h)

GENERATE_ENUM_SERIALIZATION(queue_schema.h)

END()

RECURSE_FOR_TESTS(
    ut
    yc_search_ut
)