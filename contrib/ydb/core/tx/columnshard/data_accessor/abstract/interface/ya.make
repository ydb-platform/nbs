LIBRARY()

SRCDIR(contrib/ydb/core/tx/columnshard/data_accessor/abstract)

SRCS(
    constructor.h
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/library/actors/core
    contrib/ydb/library/accessor
    contrib/ydb/library/conclusion
    contrib/ydb/services/bg_tasks/abstract
)

END()
