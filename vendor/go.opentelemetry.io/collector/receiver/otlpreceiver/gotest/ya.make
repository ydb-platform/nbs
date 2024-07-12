GO_TEST_FOR(vendor/go.opentelemetry.io/collector/receiver/otlpreceiver)

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

DATA(
    arcadia/vendor/go.opentelemetry.io/collector/receiver/otlpreceiver/testdata
)

TEST_CWD(vendor/go.opentelemetry.io/collector/receiver/otlpreceiver)

END()
