GO_TEST_FOR(vendor/go.opentelemetry.io/collector/config/configtls)

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

DATA(
    arcadia/vendor/go.opentelemetry.io/collector/config/configtls/testdata
)

TEST_CWD(vendor/go.opentelemetry.io/collector/config/configtls)

GO_SKIP_TESTS(
    TestOptionsToConfig
    TestEagerlyLoadCertificate
    TestCertificateReload
)

END()
