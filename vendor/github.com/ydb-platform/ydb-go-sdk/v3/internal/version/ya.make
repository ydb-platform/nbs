GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    parse.go
    version.go
)

GO_TEST_SRCS(parse_test.go)

END()

RECURSE(
    gotest
)
