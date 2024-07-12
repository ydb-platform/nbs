GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    random.go
)

GO_TEST_SRCS(random_test.go)

END()

RECURSE(
    gotest
)
