GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    runenames.go
    tables15.0.0.go
)

GO_TEST_SRCS(runenames_test.go)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
    gotest
)
