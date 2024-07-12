GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    bigtable_instance_admin.pb.go
    bigtable_table_admin.pb.go
    common.pb.go
    instance.pb.go
    table.pb.go
)

END()
