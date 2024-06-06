GO_TEST_FOR(vendor/go.opentelemetry.io/otel/exporters/prometheus)

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

DATA(
    arcadia/vendor/go.opentelemetry.io/otel/exporters/prometheus/testdata
)

TEST_CWD(vendor/go.opentelemetry.io/otel/exporters/prometheus)

END()
