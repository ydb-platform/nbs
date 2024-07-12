GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    doc.go
    format.go
    generator.go
    helpers.go
    naming.go
    template.go
    types.go
)

GO_TEST_SRCS(
    cycle_test.go
    helpers_test.go
    naming_test.go
    template_fuzz_test.go
    template_test.go
    types_test.go
)

GO_XTEST_SRCS(
    format_test.go
    generator_test.go
)

END()

RECURSE(
    gotest
)
