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
    metrics
    qcow2
    vhd
    vmdk
)

RECURSE_FOR_TESTS(
    tests
)
