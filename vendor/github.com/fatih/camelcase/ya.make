GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    camelcase.go
)

GO_TEST_SRCS(camelcase_test.go)

END()

RECURSE(
    gotest
)
