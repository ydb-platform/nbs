LIBRARY()

SRCS(
    create_table.cpp
    drop_table.cpp
    alter_table.cpp
    create_store.cpp
    drop_store.cpp
    alter_store.cpp
)

PEERDIR(
    contrib/ydb/core/mind/hive
    contrib/ydb/services/bg_tasks
)

YQL_LAST_ABI_VERSION()

END()
