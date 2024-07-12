GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    decoder.go
    encoder.go
    error.go
)

GO_TEST_SRCS(
    decoder_test.go
    encoder_test.go
)

END()

RECURSE(
    dotenv
    gotest
    hcl
    ini
    javaproperties
    json
    toml
    yaml
)
