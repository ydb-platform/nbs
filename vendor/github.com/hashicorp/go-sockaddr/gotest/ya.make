GO_TEST_FOR(vendor/github.com/hashicorp/go-sockaddr)

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

GO_SKIP_TESTS(
    TestGetAllInterfaces
    TestGetDefaultInterface
    TestGetDefaultInterfaces
)

END()
