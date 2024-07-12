GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    ast.go
    clone.go
    fodder.go
    identifier_set.go
    location.go
    util.go
)

GO_TEST_SRCS(util_test.go)

END()

RECURSE(
    gotest
)
