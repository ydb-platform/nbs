GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    schema.go
)

GO_TEST_SRCS(
    # schema_test.go
)

END()

RECURSE(
    gotest
)
