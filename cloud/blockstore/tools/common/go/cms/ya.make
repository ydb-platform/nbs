GO_LIBRARY()

SRCS(
    cms.go
)

GO_TEST_SRCS(
    cms_test.go
)

END()

RECURSE_FOR_TESTS(gotest)
