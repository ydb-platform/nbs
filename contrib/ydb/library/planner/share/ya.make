LIBRARY()

PEERDIR(
    contrib/ydb/library/planner/share/protos
    contrib/ydb/library/analytics
    library/cpp/lwtrace
    library/cpp/monlib/encode/legacy_protobuf/protos
)

SRCS(
    account.cpp
    group.cpp
    history.cpp
    node.cpp
    probes.cpp
    shareplanner.cpp
    # utilization.cpp
)

END()

RECURSE(
    ut
)
