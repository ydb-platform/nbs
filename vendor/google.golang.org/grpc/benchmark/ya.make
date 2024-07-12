GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    benchmark.go
)

END()

RECURSE(
    benchmain
    benchresult
    client
    flags
    latency
    # primitives
    server
    stats
    worker
)
