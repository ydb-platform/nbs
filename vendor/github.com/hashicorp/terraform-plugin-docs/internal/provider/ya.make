GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    generate.go
    template.go
    util.go
    validate.go
)

GO_TEST_SRCS(
    template_test.go
    util_test.go
)

END()

RECURSE(
    gotest
)
