GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    doc.go
    static_value.go
)

GO_XTEST_SRCS(static_value_test.go)

END()

RECURSE(
    gotest
)
