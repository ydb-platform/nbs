GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    bool_type.go
    bool_value.go
    doc.go
    float64_type.go
    float64_value.go
    int64_type.go
    int64_value.go
    list_type.go
    list_value.go
    map_type.go
    map_value.go
    number_type.go
    number_value.go
    object_type.go
    object_value.go
    set_type.go
    set_value.go
    string_type.go
    string_value.go
)

END()

RECURSE(
    basetypes
)
