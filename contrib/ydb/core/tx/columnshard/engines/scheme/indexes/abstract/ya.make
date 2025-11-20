LIBRARY()

SRCS(
    constructor.cpp
    meta.cpp
    checker.cpp
    program.cpp
    GLOBAL composite.cpp
    simple.cpp
)

PEERDIR(
    contrib/ydb/core/formats/arrow
    contrib/ydb/library/formats/arrow/protos
)

YQL_LAST_ABI_VERSION()

END()
