GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    doc.go
    permanent.go
    signalerrors.go
)

GO_TEST_SRCS(
    permanent_test.go
    signalerrors_test.go
)

END()

RECURSE(
    gotest
)
