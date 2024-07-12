GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    compare_types.go
    conversion.go
    conversion_capsule.go
    conversion_collection.go
    conversion_dynamic.go
    conversion_object.go
    conversion_primitive.go
    conversion_tuple.go
    doc.go
    mismatch_msg.go
    public.go
    sort_types.go
    unify.go
)

GO_TEST_SRCS(
    compare_types_test.go
    conversion_capsule_test.go
    mismatch_msg_test.go
    public_test.go
    sort_types_test.go
    unify_test.go
)

END()

RECURSE(
    gotest
)
