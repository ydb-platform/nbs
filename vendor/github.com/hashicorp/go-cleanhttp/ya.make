GO_LIBRARY()

LICENSE(MPL-2.0)

VERSION(v0.5.2)

SRCS(
    cleanhttp.go
    doc.go
    handlers.go
)

GO_TEST_SRCS(handlers_test.go)

END()

RECURSE(
    gotest
)
