LIBRARY(library-formats-arrow-accessor-composite)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/formats/arrow/filter
    contrib/ydb/library/formats/arrow/common
)

SRCS(
    accessor.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
