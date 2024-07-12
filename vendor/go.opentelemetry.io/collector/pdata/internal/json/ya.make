GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    attribute.go
    enum.go
    json.go
    number.go
    resource.go
    scope.go
)

GO_TEST_SRCS(
    attribute_test.go
    enum_test.go
    number_test.go
    resource_test.go
    scope_test.go
)

END()

RECURSE(
    gotest
)
