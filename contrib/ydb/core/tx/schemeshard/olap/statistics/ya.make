LIBRARY()

SRCS(
    schema.cpp
    update.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/core/tx/schemeshard/olap/common
)

END()
