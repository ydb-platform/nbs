GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    fakes2av2.go
)

GO_TEST_SRCS(fakes2av2_test.go)

GO_EMBED_PATTERN(testdata/client_root_cert.der)

GO_EMBED_PATTERN(testdata/client_root_cert.pem)

GO_EMBED_PATTERN(testdata/client_root_key.pem)

GO_EMBED_PATTERN(testdata/server_root_cert.der)

GO_EMBED_PATTERN(testdata/server_root_cert.pem)

GO_EMBED_PATTERN(testdata/server_root_key.pem)

END()

RECURSE(
    gotest
)
