LIBRARY()

SRCS(
    flame_graph_builder.cpp
    flame_graph_entry.cpp
)

PEERDIR(
    contrib/ydb/public/lib/ydb_cli/common
)

END()
