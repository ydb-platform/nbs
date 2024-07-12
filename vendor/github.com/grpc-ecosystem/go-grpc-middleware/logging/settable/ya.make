GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    doc.go
    logsettable.go
)

GO_XTEST_SRCS(logsettable_test.go)

END()

RECURSE(
    gotest
)
