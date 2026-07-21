LIBRARY()

SRCS(
    database_type.h
    database_type.cpp
    db_async_resolver.h
    mdb_endpoint_generator.h
)

PEERDIR(
    library/cpp/threading/future
    contrib/ydb/library/yql/providers/common/proto
    contrib/ydb/library/yql/public/issue
)

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(database_type.h)

END()
