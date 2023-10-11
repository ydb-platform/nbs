OWNER(g:cloud-nbs)

GO_LIBRARY()

SRCS(
    common.go
    image_map_reader.go
)

END()

RECURSE_FOR_TESTS(
    tests
)
