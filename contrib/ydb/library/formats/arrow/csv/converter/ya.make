LIBRARY()

SRCS(
    csv_arrow.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/public/api/protos
    contrib/ydb/public/lib/scheme_types
    contrib/ydb/library/yql/types/uuid
)

END()
