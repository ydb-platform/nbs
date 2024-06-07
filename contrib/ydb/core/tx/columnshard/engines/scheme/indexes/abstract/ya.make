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
    contrib/ydb/core/protos
    contrib/ydb/core/formats/arrow
)

YQL_LAST_ABI_VERSION()

END()
