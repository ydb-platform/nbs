GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    descriptor.go
)

GO_TEST_SRCS(descriptor_test.go)

END()

RECURSE(
    gotest
)
