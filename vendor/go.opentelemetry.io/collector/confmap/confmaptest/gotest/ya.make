GO_TEST_FOR(vendor/go.opentelemetry.io/collector/confmap/confmaptest)

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

DATA(
    arcadia/vendor/go.opentelemetry.io/collector/confmap/confmaptest/testdata
)

TEST_CWD(vendor/go.opentelemetry.io/collector/confmap/confmaptest)

END()
