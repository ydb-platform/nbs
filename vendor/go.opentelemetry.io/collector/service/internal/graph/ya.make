GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    graph.go
    nodes.go
    zpages.go
)

GO_TEST_SRCS(graph_test.go)

END()

RECURSE(
    gotest
)
