GO_LIBRARY()

SRCS(
    chunk_map_reader.go
    image_reader.go
    raw_image_map_reader.go
    source.go
    formats.go
)

END()

RECURSE(
    common
    qcow2
    vmdk
)

RECURSE_FOR_TESTS(
    tests
)
