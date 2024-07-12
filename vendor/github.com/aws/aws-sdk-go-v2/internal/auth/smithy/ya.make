GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    bearer_token_adapter.go
    bearer_token_signer_adapter.go
    credentials_adapter.go
    smithy.go
    v4signer_adapter.go
)

END()
