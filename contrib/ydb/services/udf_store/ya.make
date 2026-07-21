LIBRARY()
YQL_LAST_ABI_VERSION()

SRCS(
    service.cpp
    store_initializer.cpp
    kv_body_store.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/core/base
    contrib/ydb/core/keyvalue
    contrib/ydb/core/tx/scheme_cache
    contrib/ydb/library/aclib
    contrib/ydb/library/table_creator
    contrib/ydb/services/udf_store/metadata_subscription
    contrib/ydb/services/metadata/request
    contrib/ydb/services/metadata/abstract
    contrib/ydb/services/metadata/manager
    contrib/ydb/services/metadata
    contrib/ydb/library/yql/minikql
    library/cpp/digest/md5
)

END()
