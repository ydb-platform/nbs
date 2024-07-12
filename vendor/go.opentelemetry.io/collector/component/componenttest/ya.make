GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    configtest.go
    doc.go
    nop_host.go
    nop_telemetry.go
)

GO_TEST_SRCS(
    configtest_test.go
    nop_host_test.go
    nop_telemetry_test.go
)

END()

RECURSE(
    gotest
)
