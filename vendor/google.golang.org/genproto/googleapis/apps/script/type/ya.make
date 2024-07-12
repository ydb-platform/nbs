GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    addon_widget_set.pb.go
    extension_point.pb.go
    script_manifest.pb.go
)

END()

RECURSE(
    calendar
    docs
    drive
    gmail
    sheets
    slides
)
