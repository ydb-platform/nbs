GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    configtest.go
    doc.go
)

GO_TEST_SRCS(configtest_test.go)

END()

RECURSE(
    gotest
)
