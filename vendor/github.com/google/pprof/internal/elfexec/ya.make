GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    elfexec.go
)

GO_TEST_SRCS(elfexec_test.go)

END()

RECURSE(
    gotest
)
