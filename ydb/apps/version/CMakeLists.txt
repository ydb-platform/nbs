
add_library(apps-version)

target_link_libraries(apps-version PUBLIC
  ydb-core-driver_lib-version
  contrib-libs-linux-headers
  contrib-libs-cxxsupp
  yutil
  api-grpc-draft
  cpp-client-ydb_table
  client-ydb_types-operation
  cpp-client-ydb_value
)

target_sources(apps-version PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/apps/version/version_definition.cpp
)
