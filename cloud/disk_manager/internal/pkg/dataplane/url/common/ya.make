OWNER(g:cloud-nbs)

GO_LIBRARY()

SRCS(
    errors.go
    http_client.go
    image_map.go
    reader.go
)

GO_TEST_SRCS(
    http_client_test.go
    reader_test.go
)

END()

RECURSE(
    cache
    testing
)

RECURSE_FOR_TESTS(
    tests
)
