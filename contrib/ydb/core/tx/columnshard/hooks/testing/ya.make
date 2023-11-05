LIBRARY()

SRCS(
    controller.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/hooks/abstract
    contrib/ydb/core/tx/columnshard/engines/changes
)

END()
