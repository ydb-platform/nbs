GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    label_iter.go
    svchost.go
)

GO_TEST_SRCS(svchost_test.go)

END()

RECURSE(
    gotest
)
