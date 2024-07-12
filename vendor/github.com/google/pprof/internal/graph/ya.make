GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    dotgraph.go
    graph.go
)

GO_TEST_SRCS(
    dotgraph_test.go
    graph_test.go
)

END()

RECURSE(
    gotest
)
