GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    testr.go
)

GO_TEST_SRCS(
    testr_fuzz_test.go
    testr_test.go
)

END()

RECURSE(
    gotest
)
