GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    hcvault_aead.go
    hcvault_client.go
)

GO_XTEST_SRCS(
    hcvault_aead_test.go
    hcvault_client_test.go
)

END()

RECURSE(
    gotest
)
