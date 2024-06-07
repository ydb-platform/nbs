PROGRAM(example_02_discovery)

ALLOCATOR(LF)

SRCS(
    endpoint.cpp
    lookup.cpp
    main.cpp
    publish.cpp
    replica.cpp
    services.h
)

SRCS(
    protocol.proto
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/dnsresolver
    contrib/ydb/library/actors/interconnect
    contrib/ydb/library/actors/http
)

END()
