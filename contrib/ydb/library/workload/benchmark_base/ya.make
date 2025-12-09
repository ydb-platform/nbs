LIBRARY()

SUBSCRIBER(g:kikimr)

SRCS(
    workload.cpp
    state.cpp
)

PEERDIR(
    contrib/ydb/library/accessor
    contrib/ydb/library/workload/abstract
    contrib/ydb/library/yaml_json
    contrib/ydb/public/api/protos
)

GENERATE_ENUM_SERIALIZATION_WITH_HEADER(workload.h)

END()

RECURSE_FOR_TESTS(
    ut
)
