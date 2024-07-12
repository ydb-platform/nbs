GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    doc.go
)

GO_XTEST_SRCS(unit_test.go)

END()

RECURSE(
    gotest
)
