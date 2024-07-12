GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    gcp_kms_aead.go
    gcp_kms_client.go
)

GO_XTEST_SRCS(
    gcp_kms_aead_test.go
    gcp_kms_client_test.go
)

END()

RECURSE(
    # gotest
)
