LIBRARY()

SRCS(
    abstract.cpp
    execute.cpp
    actualization.cpp
    compaction.cpp
    executor.cpp
    variator.cpp
    select.cpp
    bulk_upsert.cpp
)

PEERDIR(
    contrib/ydb/core/testlib
)

YQL_LAST_ABI_VERSION()

END()
