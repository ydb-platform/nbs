GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    doc.go
)

GO_TEST_SRCS(
    convertfunc_test.go
    hcldec_into_expr_test.go
    terraformlike_test.go
)

END()

RECURSE(
    gotest
)
