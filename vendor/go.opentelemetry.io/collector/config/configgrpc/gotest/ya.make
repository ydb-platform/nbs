GO_TEST_FOR(vendor/go.opentelemetry.io/collector/config/configgrpc)

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

DATA(
    arcadia/vendor/go.opentelemetry.io/collector/config/configgrpc/testdata
)

TEST_CWD(vendor/go.opentelemetry.io/collector/config/configgrpc)

GO_SKIP_TESTS(TestHttpReception)

END()
