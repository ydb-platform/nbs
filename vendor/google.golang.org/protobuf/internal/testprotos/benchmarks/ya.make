GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    benchmarks.pb.go
)

END()

RECURSE(
    datasets
    micro
)
