LIBRARY()

SRCS(
    schema.cpp
    update.cpp
)

PEERDIR(
    contrib/ydb/core/tx/schemeshard/olap/columns
    contrib/ydb/core/tx/schemeshard/olap/indexes
)

END()
