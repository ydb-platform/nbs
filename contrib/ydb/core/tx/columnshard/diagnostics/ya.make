LIBRARY()

SRCS(
    scan_diagnostics_actor.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/libs/opentelemetry-proto
    contrib/ydb/core/base/generated
    contrib/ydb/core/control/lib/generated
    contrib/ydb/core/tx/columnshard/engines/protos  # stopgap: columnshard_private_events.h transitively requires engines/protos; direct columnshard dep would create a cycle
    contrib/ydb/library/aclib/protos
    contrib/ydb/library/actors/core
    contrib/ydb/library/yql/public/issue/protos
)

RESOURCE(
    viz-global.js viz-global.js
)

END()
