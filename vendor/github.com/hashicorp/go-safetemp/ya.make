GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    safetemp.go
)

GO_TEST_SRCS(safetemp_test.go)

END()

RECURSE(
    gotest
)
