LIBRARY()

SRCS(
    local_pgwire_connection.cpp
    local_pgwire.cpp
    local_pgwire.h
    local_pgwire_util.cpp
    local_pgwire_util.h
    log_impl.h
    pgwire_kqp_proxy.cpp
    sql_parser.cpp
    sql_parser.h
)

PEERDIR(
    library/cpp/actors/core
    contrib/ydb/library/yql/parser/pg_wrapper/interface
    contrib/ydb/core/kqp/common/events
    contrib/ydb/core/kqp/common/simple
    contrib/ydb/core/kqp/executer_actor
    contrib/ydb/core/grpc_services
    contrib/ydb/core/grpc_services/local_rpc
    contrib/ydb/core/protos
    contrib/ydb/core/pgproxy
    contrib/ydb/core/ydb_convert
    contrib/ydb/public/api/grpc
    contrib/ydb/public/lib/operation_id/protos
)

YQL_LAST_ABI_VERSION()

END()
