GO_PROGRAM()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    generic_go118.go
    mockgen.go
    parse.go
    reflect.go
    version.go
)

GO_TEST_SRCS(
    mockgen_test.go
    parse_test.go
)

END()

RECURSE(
    gotest
    internal
    model
)
