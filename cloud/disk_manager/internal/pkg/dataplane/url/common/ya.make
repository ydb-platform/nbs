OWNER(g:cloud-nbs)

GO_LIBRARY()

SRCS(
    cache.go
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
    testing
)

RECURSE_FOR_TESTS(
    tests
)
