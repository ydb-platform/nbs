GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    handler.go
    span.go
    tracer.go
)

GO_XTEST_SRCS(
    span_test.go
    tracer_test.go
)

END()

RECURSE(
    gotest
    oc2otel
    ocmetric
    otel2oc
)
