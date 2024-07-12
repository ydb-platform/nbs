GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    doc.go
    entry.go
    field.go
    fieldtype.go
    fieldtype_default.go
    fieldtypeset.go
    fieldtypespace.go
    ifd.go
    misc.go
    tag.go
    tag_baseline.go
    tag_extended.go
    tag_private.go
    tagset.go
    tagspace.go
    tiff.go
    unmarshal.go
)

END()

RECURSE(
    bigtiff
)
