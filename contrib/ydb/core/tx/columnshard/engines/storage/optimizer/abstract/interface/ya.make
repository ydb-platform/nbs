LIBRARY()

SRCDIR(contrib/ydb/core/tx/columnshard/engines/storage/optimizer/abstract)

SRCS(
    optimizer.h
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/protos
    contrib/ydb/core/tx/columnshard/common
    contrib/ydb/core/tx/columnshard/counters
    contrib/ydb/library/accessor
    contrib/ydb/library/conclusion
    contrib/ydb/services/bg_tasks/abstract
)

END()
