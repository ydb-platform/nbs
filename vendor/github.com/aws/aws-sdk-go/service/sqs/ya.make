GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    api.go
    checksums.go
    customizations.go
    doc.go
    errors.go
    service.go
)

GO_XTEST_SRCS(checksums_test.go)

END()

RECURSE(
    gotest
    sqsiface
)
