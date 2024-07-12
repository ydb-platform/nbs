GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    downscoping.go
)

GO_TEST_SRCS(downscoping_test.go)

GO_XTEST_SRCS(tokenbroker_test.go)

END()

RECURSE(
    gotest
)
