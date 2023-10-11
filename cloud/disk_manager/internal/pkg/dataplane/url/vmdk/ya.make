OWNER(g:cloud-nbs)

GO_LIBRARY()

SRCS(
    image_map_reader.go
    marker.go
    header.go
    grain.go
)

END()

RECURSE_FOR_TESTS(
    tests
)
