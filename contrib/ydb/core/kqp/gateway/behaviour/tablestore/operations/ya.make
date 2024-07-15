LIBRARY()

SRCS(
    abstract.cpp
    GLOBAL add_column.cpp
    GLOBAL alter_column.cpp
    GLOBAL drop_column.cpp
    GLOBAL upsert_index.cpp
    GLOBAL drop_index.cpp
    GLOBAL upsert_stat.cpp
    GLOBAL drop_stat.cpp
    GLOBAL upsert_opt.cpp
)

PEERDIR(
    contrib/ydb/services/metadata/manager
    contrib/ydb/core/formats/arrow/serializer
    contrib/ydb/core/tx/columnshard/engines/scheme/statistics/abstract
    contrib/ydb/core/kqp/gateway/utils
    contrib/ydb/core/protos
)

YQL_LAST_ABI_VERSION()

END()
