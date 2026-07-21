LIBRARY()

SRCS(
    column_families.cpp
    compression.cpp
    kesus_description.cpp
    table_settings.cpp
    table_description.cpp
    table_profiles.cpp
    topic_description.cpp
    replication_description.cpp
    external_data_source_description.cpp
    external_table_description.cpp
    ydb_convert.cpp
    tx_proxy_status.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/engine
    contrib/ydb/core/formats/arrow/switch
    contrib/ydb/library/yql/core
    contrib/ydb/core/local_indexes/bloom
    contrib/ydb/core/persqueue/events
    contrib/ydb/core/protos
    contrib/ydb/core/scheme
    contrib/ydb/core/util
    contrib/ydb/library/yql/types/binary_json
    contrib/ydb/library/yql/providers/result/expr_nodes
    contrib/ydb/library/yql/types/dynumber
    contrib/ydb/library/conclusion
    contrib/ydb/library/mkql_proto/protos
    contrib/ydb/library/yql/minikql/dom
    contrib/ydb/library/yql/public/udf
    contrib/ydb/public/api/protos
)

GENERATE_ENUM_SERIALIZATION(table_description.h)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
