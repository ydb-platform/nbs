RECURSE(
    object_storage
)

LIBRARY()

SRCS(
    external_data_source.cpp
    external_source_factory.cpp
    object_storage.cpp
    validation_functions.cpp
)

PEERDIR(
    contrib/ydb/core/external_sources/object_storage/inference
    contrib/ydb/library/actors/http
    contrib/ydb/library/yql/providers/common/gateway
    library/cpp/regex/pcre
    library/cpp/scheme
    contrib/ydb/core/base
    contrib/ydb/core/protos
    contrib/ydb/library/yql/providers/common/db_id_async_resolver
    contrib/ydb/library/yql/providers/s3/common
    contrib/ydb/library/yql/providers/s3/object_listers
    contrib/ydb/library/yql/providers/s3/path_generator
    contrib/ydb/library/yql/providers/common/gateway
    contrib/ydb/library/yql/public/issue
    contrib/ydb/public/sdk/cpp/client/ydb_params
    contrib/ydb/public/sdk/cpp/client/ydb_value
)

END()

RECURSE_FOR_TESTS(
    ut
)

RECURSE(
    hive_metastore
)
