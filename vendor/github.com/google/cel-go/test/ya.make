GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause
)

SRCS(
    compare.go
    expr.go
)

END()

RECURSE(
    bench
    proto2pb
    proto3pb
)
