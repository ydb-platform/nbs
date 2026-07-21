LIBRARY()

SRCS(
    partitioning.cpp
    translation_settings.cpp
    translator.cpp
)

PEERDIR(
    library/cpp/deprecated/split
    library/cpp/json
    contrib/ydb/library/yql/public/issue
    contrib/ydb/library/yql/public/langver
    contrib/ydb/library/yql/public/udf_meta
    contrib/ydb/library/yql/core/issue
    contrib/ydb/library/yql/core/pg_settings
    contrib/ydb/library/yql/public/issue/protos
    contrib/ydb/library/yql/sql/settings/flags
    contrib/ydb/library/yql/utils
)

END()

RECURSE(
    flags
)
