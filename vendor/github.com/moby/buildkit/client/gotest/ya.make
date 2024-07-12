GO_TEST_FOR(vendor/github.com/moby/buildkit/client)

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

# NB: test requires "registry" binary. Dockerfile has some references of copying this binary

GO_SKIP_TESTS(
    TestClientGatewayIntegration
    TestIntegration
)

END()
