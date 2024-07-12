GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    aws_kms_aead.go
    aws_kms_client.go
)

GO_XTEST_SRCS(
    aws_kms_aead_test.go
    aws_kms_client_test.go
)

END()

RECURSE(
    # gotest
)
