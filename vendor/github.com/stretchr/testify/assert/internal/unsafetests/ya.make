GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    doc.go
)

GO_XTEST_SRCS(unsafetests_test.go)

END()

RECURSE(
    gotest
)
