GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    doc.go
    helpers.go
    in.go
    out.go
    type_implied.go
)

GO_TEST_SRCS(
    in_test.go
    out_test.go
    type_implied_test.go
)

END()

RECURSE(
    gotest
)
