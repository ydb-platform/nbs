GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    block_labels.go
    decode.go
    doc.go
    gob.go
    public.go
    schema.go
    spec.go
    variables.go
)

GO_TEST_SRCS(
    public_test.go
    spec_test.go
    variables_test.go
)

END()

RECURSE(
    gotest
)
