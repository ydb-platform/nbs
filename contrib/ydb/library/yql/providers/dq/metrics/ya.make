LIBRARY()

SET(
    SOURCE
    metrics_printer.cpp
)

SRCS(
    ${SOURCE}
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/yql/providers/solomon/async_io
)

YQL_LAST_ABI_VERSION()

END()
