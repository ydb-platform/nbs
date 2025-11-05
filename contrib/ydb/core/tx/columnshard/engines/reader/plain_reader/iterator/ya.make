LIBRARY()

SRCS(
    scanner.cpp
    constructor.cpp
    source.cpp
    interval.cpp
    fetched_data.cpp
    plain_read_data.cpp
    merge.cpp
    columns_set.cpp
    context.cpp
    fetching.cpp
    iterator.cpp
)

PEERDIR(
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/tx/columnshard/blobs_action
    contrib/ydb/core/tx/conveyor/usage
    contrib/ydb/core/tx/limiter/grouped_memory/usage
)

GENERATE_ENUM_SERIALIZATION(columns_set.h)

END()
