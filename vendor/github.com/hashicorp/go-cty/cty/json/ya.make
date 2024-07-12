GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    doc.go
    marshal.go
    simple.go
    type.go
    type_implied.go
    unmarshal.go
    value.go
)

GO_TEST_SRCS(
    simple_test.go
    type_implied_test.go
    value_test.go
)

END()

RECURSE(
    gotest
)
