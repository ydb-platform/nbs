GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    driver.go
)

GO_TEST_SRCS(
    # dirver_test.go
)

END()

RECURSE(
    gotest
)
