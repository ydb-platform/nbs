LIBRARY()

SRCS(
    iscan.cpp
)

PEERDIR(
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/tx/columnshard/engines/protos  # stopgap: columnshard_private_events.h transitively requires engines/protos; direct columnshard dep would create a cycle
    contrib/ydb/core/tx/datashard
    contrib/ydb/library/actors/core
    contrib/ydb/library/services
    contrib/ydb/library/signals
)

END()

RECURSE_FOR_TESTS(
    ut
)