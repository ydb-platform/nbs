GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    confmap.go
    converter.go
    expand.go
    provider.go
    resolver.go
)

GO_TEST_SRCS(
    confmap_test.go
    expand_test.go
    provider_test.go
    resolver_test.go
)

END()

RECURSE(
    confmaptest
    converter
    gotest
    internal
    provider
)
