GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    defaults.go
    doc.go
    get_type.go
    public.go
    type_type.go
)

GO_TEST_SRCS(
    defaults_test.go
    get_type_test.go
    type_string_test.go
    type_type_test.go
)

END()

RECURSE(
    gotest
)
