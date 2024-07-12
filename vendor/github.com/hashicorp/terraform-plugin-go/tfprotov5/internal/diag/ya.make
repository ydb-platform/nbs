GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    diagnostics.go
    doc.go
)

GO_XTEST_SRCS(diagnostics_test.go)

END()

RECURSE(
    gotest
)
