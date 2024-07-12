GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    slices.go
)

GO_TEST_SRCS(slices_test.go)

END()

RECURSE(
    gotest
)
