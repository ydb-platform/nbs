LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/library/actors/core
)

SRCS(
    switch_type.cpp
    compare.cpp
)

END()
