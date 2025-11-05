PROGRAM()

SRCS(
    appdata.h
    log_impl.h
    main.cpp
    pg_ydb_connection.cpp
    pg_ydb_connection.h
    pg_ydb_proxy.cpp
    pg_ydb_proxy.h
    pgwire.cpp
    pgwire.h
    signals.h
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/pgproxy
    contrib/ydb/core/local_pgwire
    contrib/ydb/core/protos
    contrib/ydb/public/api/grpc
    contrib/ydb/public/sdk/cpp/client/ydb_driver
    contrib/ydb/public/sdk/cpp/client/draft
    contrib/ydb/library/yql/parser/pg_wrapper
    contrib/ydb/library/yql/public/udf/service/exception_policy
)

END()
