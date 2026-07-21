LIBRARY()

SRCS(
    actor.cpp
    counters.cpp
    task.cpp
    events.cpp
    container.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/core/tx/columnshard/engines/protos  # stopgap: columnshard_private_events.h transitively requires engines/protos; direct columnshard dep would create a cycle
    contrib/ydb/library/actors/core
    contrib/ydb/core/tablet_flat
)

END()
