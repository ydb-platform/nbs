GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    config.go
    doc.go
    factory.go
    zpagesextension.go
)

GO_TEST_SRCS(
    config_test.go
    factory_test.go
    zpagesextension_test.go
)

END()

RECURSE(
    gotest
)
