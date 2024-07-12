GO_TEST_FOR(vendor/go.opentelemetry.io/collector/obsreport/obsreporttest)

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

DATA(
    arcadia/vendor/go.opentelemetry.io/collector/obsreport/obsreporttest/testdata
)

TEST_CWD(vendor/go.opentelemetry.io/collector/obsreport/obsreporttest)

END()
