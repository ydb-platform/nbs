GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    diagnostics.go
    schema.go
)

GO_TEST_SRCS(
    diagnostics_test.go
    schema_test.go
)

END()

RECURSE(
    gotest
)
