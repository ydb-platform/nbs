LIBRARY()

SRCS(
    controller.cpp
    ro_controller.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/hooks/abstract
    contrib/ydb/core/tx/columnshard/engines/changes
)

END()
