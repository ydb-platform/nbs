GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(path.go)

GO_TEST_SRCS(path_test.go)

END()

RECURSE(gotest)
