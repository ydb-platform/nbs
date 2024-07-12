GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    input.go
)

END()

RECURSE(
    reflect_output
    source_output
)
