LIBRARY()

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/change_exchange
    contrib/ydb/core/protos
    contrib/ydb/core/scheme
    contrib/ydb/core/scheme_types
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/io_formats/cell_maker
    contrib/ydb/core/tx/replication/ydb_proxy
    contrib/ydb/library/actors/core
    contrib/ydb/library/services
    library/cpp/json
)

SRCS(
    json_change_record.cpp
    service.cpp
    table_writer.cpp
    topic_reader.cpp
    worker.cpp
)

YQL_LAST_ABI_VERSION()

END()
