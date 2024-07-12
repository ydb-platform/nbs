GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    remotesigner.go
)

GO_TEST_SRCS(remotesigner_test.go)

GO_TEST_EMBED_PATTERN(testdata/client_cert.der)

GO_TEST_EMBED_PATTERN(testdata/client_cert.pem)

GO_TEST_EMBED_PATTERN(testdata/client_key.pem)

GO_TEST_EMBED_PATTERN(testdata/server_cert.der)

GO_TEST_EMBED_PATTERN(testdata/server_cert.pem)

GO_TEST_EMBED_PATTERN(testdata/server_key.pem)

END()

RECURSE(
    gotest
)
