GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause
)

SRCS(
    container.go
)

GO_TEST_SRCS(container_test.go)

END()

RECURSE(
    gotest
)
