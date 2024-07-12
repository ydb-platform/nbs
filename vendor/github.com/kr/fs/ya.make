GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    filesystem.go
    walk.go
)

GO_XTEST_SRCS(
    example_test.go
    # walk_test.go
)

END()

RECURSE(
    gotest
)
