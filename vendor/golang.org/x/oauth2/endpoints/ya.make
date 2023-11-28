GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    endpoints.go
)

GO_TEST_SRCS(endpoints_test.go)

END()

RECURSE(
    gotest
)
