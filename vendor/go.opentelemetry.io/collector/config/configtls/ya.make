GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    clientcasfilereloader.go
    configtls.go
    doc.go
)

GO_TEST_SRCS(
    clientcasfilereloader_test.go
    configtls_test.go
)

END()

RECURSE(
    gotest
)
