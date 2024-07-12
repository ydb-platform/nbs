GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    autoneg.go
)

GO_TEST_SRCS(autoneg_test.go)

END()

RECURSE(
    gotest
)
