GO_LIBRARY()

LICENSE(Apache-2.0)

SUBSCRIBER(g:go-contrib)

SRCS(
    doc.go
    version.go
)

GO_XTEST_SRCS(version_test.go)

END()

RECURSE(
    gotest
)
