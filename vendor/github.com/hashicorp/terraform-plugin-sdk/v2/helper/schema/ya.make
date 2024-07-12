GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    context.go
    core_schema.go
    data_source_resource_shim.go
    equal.go
    field_reader.go
    field_reader_config.go
    field_reader_diff.go
    field_reader_map.go
    field_reader_multi.go
    field_writer.go
    field_writer_map.go
    getsource_string.go
    grpc_provider.go
    json.go
    provider.go
    resource.go
    resource_data.go
    resource_data_get_source.go
    resource_diff.go
    resource_importer.go
    resource_timeout.go
    schema.go
    serialize.go
    set.go
    shims.go
    testing.go
    unknown.go
    valuetype.go
    valuetype_string.go
)

GO_TEST_SRCS(
    core_schema_test.go
    field_reader_config_test.go
    field_reader_diff_test.go
    field_reader_map_test.go
    field_reader_multi_test.go
    field_reader_test.go
    field_writer_map_test.go
    grpc_provider_test.go
    provider_test.go
    resource_data_test.go
    resource_diff_test.go
    resource_importer_test.go
    resource_test.go
    resource_timeout_test.go
    schema_test.go
    serialize_test.go
    set_test.go
    shims_test.go
    unknown_test.go
)

END()

RECURSE(
    gotest
)
