GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    edges.go
    iradix.go
    iter.go
    node.go
    raw_iter.go
    reverse_iter.go
)

GO_TEST_SRCS(
    iradix_test.go
    node_test.go
    reverse_iter_test.go
)

END()

RECURSE(
    gotest
)
