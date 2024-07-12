GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    context.go
    doc.go
    fieldextractor.go
    interceptors.go
    options.go
)

GO_XTEST_SRCS(
    examples_test.go
    fieldextractor_test.go
    interceptors_test.go
)

END()

RECURSE(
    # gotest
    logrus
    zap
)
