LIBRARY()

SRCS(
    GLOBAL constructor.cpp
    GLOBAL operator.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/scheme/statistics/abstract
    contrib/ydb/core/tx/columnshard/engines/scheme/abstract
    contrib/ydb/core/tx/columnshard/splitter/abstract
    contrib/ydb/core/formats/arrow
)

END()
