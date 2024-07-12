GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    emf.go
    emf_test_data.go
)

GO_TEST_SRCS(emf_test.go)

END()

RECURSE(
    gotest
)
