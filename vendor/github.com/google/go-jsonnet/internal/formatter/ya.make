GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    add_plus_object.go
    enforce_comment_style.go
    enforce_max_blank_lines.go
    enforce_string_style.go
    fix_indentation.go
    fix_newlines.go
    fix_parens.go
    fix_trailing_commas.go
    jsonnetfmt.go
    no_redundant_slice_colon.go
    pretty_field_names.go
    remove_plus_object.go
    sort_imports.go
    strip.go
    unparser.go
)

END()
