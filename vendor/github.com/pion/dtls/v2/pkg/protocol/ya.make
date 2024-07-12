GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    application_data.go
    change_cipher_spec.go
    compression_method.go
    content.go
    errors.go
    version.go
)

GO_TEST_SRCS(
    change_cipher_spec_test.go
    compression_method_test.go
)

END()

RECURSE(
    alert
    extension
    gotest
    handshake
    recordlayer
)
