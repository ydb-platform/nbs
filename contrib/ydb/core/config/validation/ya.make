LIBRARY()

SRCS(
    validators.h
    validators.cpp
)

PEERDIR(
    contrib/ydb/core/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)

