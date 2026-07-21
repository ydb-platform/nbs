LIBRARY()

SRCS(
    kqp_run_script_actor_impl.cpp
    kqp_run_script_actor.cpp
    kqp_script_lease_watcher_actor.cpp
    kqp_script_result_handler.cpp
)

PEERDIR(
    library/cpp/protobuf/json
    contrib/ydb/core/base
    contrib/ydb/core/fq/libs/checkpointing/events
    contrib/ydb/core/kqp/common/events
    contrib/ydb/core/kqp/executer_actor
    contrib/ydb/core/kqp/proxy_service/proto
    contrib/ydb/core/protos
    contrib/ydb/library/actors/core
    contrib/ydb/library/yql/providers/pq/proto
    contrib/ydb/public/api/protos
)

YQL_LAST_ABI_VERSION()

END()
