LIBRARY()

SUBSCRIBER(g:kikimr)

SRCS(
    workload.cpp
    state.cpp
)

PEERDIR(
    contrib/ydb/library/accessor
    contrib/ydb/library/workload/abstract
    contrib/ydb/public/api/protos
)

GENERATE_ENUM_SERIALIZATION(workload.h)

END()

RECURSE_FOR_TESTS(
    ut
)
