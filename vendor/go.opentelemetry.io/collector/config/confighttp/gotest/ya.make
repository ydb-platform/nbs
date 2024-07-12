GO_TEST_FOR(vendor/go.opentelemetry.io/collector/config/confighttp)

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

DATA(
    arcadia/vendor/go.opentelemetry.io/collector/config/confighttp/testdata
)

TEST_CWD(vendor/go.opentelemetry.io/collector/config/confighttp)

GO_SKIP_TESTS(TestHttpReception)

END()
