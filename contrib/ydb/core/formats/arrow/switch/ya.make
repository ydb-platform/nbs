LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/scheme_types
    contrib/ydb/library/actors/core
)

SRCS(
    switch_type.cpp
    compare.cpp
)

END()
