GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    builder.go
)

GO_TEST_SRCS(builder_test.go)

END()

RECURSE(
    gotest
)
