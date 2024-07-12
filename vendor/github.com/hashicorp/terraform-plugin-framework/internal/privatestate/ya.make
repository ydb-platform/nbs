GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    data.go
    doc.go
)

GO_TEST_SRCS(data_test.go)

END()

RECURSE(
    gotest
)
