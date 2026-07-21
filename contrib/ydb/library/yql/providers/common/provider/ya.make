LIBRARY()

SRCS(
    yql_data_provider_impl.cpp
    yql_data_provider_impl.h
    yql_provider.cpp
    yql_provider.h
    yql_provider_names.h
)

PEERDIR(
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/parser/pg_catalog
)

YQL_LAST_ABI_VERSION()

END()
