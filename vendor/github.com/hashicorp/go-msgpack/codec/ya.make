GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    binc.go
    cbor.go
    decode.go
    doc.go
    encode.go
    fast-path.generated.go
    gen-helper.generated.go
    goversion_arrayof_gte_go15.go
    goversion_makemap_gte_go19.go
    goversion_unexportedembeddedptr_gte_go110.go
    goversion_vendor_gte_go17.go
    helper.go
    helper_internal.go
    helper_unsafe.go
    json.go
    msgpack.go
    rpc.go
    simple.go
)

GO_TEST_SRCS(
    cbor_test.go
    codec_test.go
    helper_test.go
    mammoth2_codecgen_generated_test.go
    mammoth2_generated_test.go
    mammoth_generated_test.go
    shared_test.go
    values_flex_test.go
    values_test.go
)

END()

RECURSE(
    gotest
)
