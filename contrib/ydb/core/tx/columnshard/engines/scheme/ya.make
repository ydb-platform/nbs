LIBRARY()

SRCS(
    abstract_scheme.cpp
    snapshot_scheme.cpp
    filtered_scheme.cpp
    index_info.cpp
    tier_info.cpp
    column_features.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/core/formats/arrow

    contrib/ydb/library/actors/core
    contrib/ydb/core/tx/columnshard/engines/scheme/indexes
)

YQL_LAST_ABI_VERSION()

END()
