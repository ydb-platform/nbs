GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(singleflight.go)

GO_TEST_SRCS(singleflight_test.go)

END()

RECURSE(gotest)