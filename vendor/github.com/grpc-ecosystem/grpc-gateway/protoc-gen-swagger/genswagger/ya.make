GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    doc.go
    generator.go
    helpers.go
    template.go
    types.go
)

GO_TEST_SRCS(template_test.go)

END()

RECURSE(
    gotest
)
