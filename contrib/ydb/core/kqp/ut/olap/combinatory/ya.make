LIBRARY()

SRCS(
    abstract.cpp
    check_counter.cpp
    execute.cpp
    actualization.cpp
    compaction.cpp
    executor.cpp
    variator.cpp
    select.cpp
    bulk_upsert.cpp
    wait_background_processes.cpp
)

PEERDIR(
    contrib/ydb/core/testlib
    contrib/ydb/core/protos
    contrib/ydb/core/kqp/ut/olap/helpers
)

YQL_LAST_ABI_VERSION()

END()
