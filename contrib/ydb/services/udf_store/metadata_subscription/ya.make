LIBRARY()
YQL_LAST_ABI_VERSION()

SRCS(
    udf_meta.cpp
    snapshot.cpp
)

GENERATE_ENUM_SERIALIZATION(udf_meta.h)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/core/base
    contrib/ydb/core/keyvalue
    contrib/ydb/core/tx/scheme_cache
    contrib/ydb/library/aclib
    contrib/ydb/library/table_creator
    contrib/ydb/services/metadata/request
    contrib/ydb/services/metadata/abstract
    contrib/ydb/services/metadata/manager
    contrib/ydb/services/metadata
    contrib/ydb/library/yql/minikql
    library/cpp/digest/md5
)

END()
