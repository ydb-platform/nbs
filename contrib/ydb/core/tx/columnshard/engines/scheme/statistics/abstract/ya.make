LIBRARY()

SRCS(
    portion_storage.cpp
    constructor.cpp
    operator.cpp
    common.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/scheme/statistics/protos
    contrib/ydb/core/tx/columnshard/engines/scheme/abstract
    contrib/libs/apache/arrow
    contrib/ydb/library/actors/core
    contrib/ydb/library/conclusion
)

GENERATE_ENUM_SERIALIZATION(common.h)

END()
