GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    ccm.go
)

GO_TEST_SRCS(ccm_test.go)

END()

RECURSE(
    gotest
)
