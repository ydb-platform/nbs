GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    feature.go
    models.go
    options.go
)

GO_TEST_SRCS(
    models_test.go
    options_test.go
)

END()

RECURSE(
    gotest
)
