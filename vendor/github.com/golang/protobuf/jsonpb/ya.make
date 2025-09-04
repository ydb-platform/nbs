GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v1.5.4)

SRCS(
    decode.go
    encode.go
    json.go
)

GO_TEST_SRCS(json_test.go)

END()

RECURSE(
    gotest
)
