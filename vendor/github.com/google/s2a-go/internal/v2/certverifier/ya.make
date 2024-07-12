GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    certverifier.go
)

GO_TEST_SRCS(certverifier_test.go)

GO_TEST_EMBED_PATTERN(testdata/client_intermediate_cert.der)

GO_TEST_EMBED_PATTERN(testdata/client_leaf_cert.der)

GO_TEST_EMBED_PATTERN(testdata/client_root_cert.der)

GO_TEST_EMBED_PATTERN(testdata/server_intermediate_cert.der)

GO_TEST_EMBED_PATTERN(testdata/server_leaf_cert.der)

GO_TEST_EMBED_PATTERN(testdata/server_root_cert.der)

END()

RECURSE(
    gotest
)
