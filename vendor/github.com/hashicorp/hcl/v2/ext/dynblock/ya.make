GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    expand_body.go
    expand_spec.go
    expr_wrap.go
    iteration.go
    public.go
    schema.go
    unknown_body.go
    variables.go
    variables_hcldec.go
)

GO_TEST_SRCS(
    expand_body_test.go
    variables_test.go
)

END()

RECURSE(
    gotest
)
