GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    doc.go
    indent.go
    wrap.go
)

GO_TEST_SRCS(
    indent_test.go
    wrap_test.go
)

END()

RECURSE(
    gotest
)
