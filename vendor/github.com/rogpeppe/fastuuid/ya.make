GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    uuid.go
)

GO_TEST_SRCS(uuid_test.go)

END()

RECURSE(
    gotest
)
