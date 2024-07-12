GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    doc.go
    partialscrapeerror.go
    scrapeerror.go
)

GO_TEST_SRCS(
    partialscrapeerror_test.go
    scrapeerror_test.go
)

END()

RECURSE(
    gotest
)
