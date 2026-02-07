LIBRARY()

SRCS(
    actor.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/reader/abstract
    contrib/ydb/core/kqp/compute_actor
    contrib/ydb/library/yql/core/issue
)

END()
