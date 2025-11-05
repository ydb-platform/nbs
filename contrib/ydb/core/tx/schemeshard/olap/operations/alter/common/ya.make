LIBRARY()

SRCS(
    update.cpp
    object.cpp
)

PEERDIR(
    contrib/ydb/core/tx/schemeshard/olap/operations/alter/abstract
)

YQL_LAST_ABI_VERSION()

END()
