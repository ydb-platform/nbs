GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    main.go
)

GO_TEST_SRCS(fuzz_test.go)

END()

RECURSE(
    gotest
)
