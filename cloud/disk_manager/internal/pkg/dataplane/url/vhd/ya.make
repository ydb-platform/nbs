GO_LIBRARY()

SRCS(
    common.go
    footer.go
    header.go
    image_map_reader.go
)

END()

RECURSE_FOR_TESTS(
    tests
)
