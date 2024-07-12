GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

GO_SKIP_TESTS(
    TestProcessCredentialsProvider_FromConfig
    TestProcessCredentialsProvider_FromConfigWithProfile
    TestProcessCredentialsProvider_FromCredentials
    TestProcessCredentialsProvider_FromCredentialsWithProfile
    TestSharedConfigCredentialSource
)

SRCS(
    config.go
    defaultsmode.go
    doc.go
    env_config.go
    generate.go
    go_module_metadata.go
    load_options.go
    local.go
    provider.go
    resolve.go
    resolve_bearer_token.go
    resolve_credentials.go
    shared_config.go
)

END()
