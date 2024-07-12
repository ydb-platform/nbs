GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    profiling.go
)

END()

RECURSE(
    cmd
    proto
    service
)
