GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    api.go
    doc.go
    errors.go
    service.go
)

GO_XTEST_SRCS(examples_test.go)

END()

RECURSE(
    gotest
    kmsiface
)
