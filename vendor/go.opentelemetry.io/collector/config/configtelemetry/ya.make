GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    configtelemetry.go
    doc.go
)

GO_TEST_SRCS(configtelemetry_test.go)

END()

RECURSE(
    gotest
)
