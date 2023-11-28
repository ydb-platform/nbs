GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    doc.go
    generator.go
    template.go
)

GO_TEST_SRCS(
    generator_test.go
    template_test.go
)

END()

RECURSE(gotest)
