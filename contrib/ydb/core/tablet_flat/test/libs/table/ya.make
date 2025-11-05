LIBRARY()

SRCS(
    misc.cpp
)

PEERDIR(
    contrib/ydb/core/tablet_flat/test/libs/rows
    contrib/ydb/core/tablet_flat/test/libs/table/model
    contrib/ydb/core/tablet_flat
)

END()

RECURSE(
    model
)
