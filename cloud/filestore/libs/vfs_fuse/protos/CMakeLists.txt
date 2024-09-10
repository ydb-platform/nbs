get_built_tool_path(
  TOOL_protoc_bin
  TOOL_protoc_dependency
  contrib/tools/protoc/bin
  protoc
)
get_built_tool_path(
  TOOL_cpp_styleguide_bin
  TOOL_cpp_styleguide_dependency
  contrib/tools/protoc/plugins/cpp_styleguide
  cpp_styleguide
)

add_library(libs-vfs_fuse-protos)
target_link_libraries(libs-vfs_fuse-protos PUBLIC
  contrib-libs-linux-headers
  contrib-libs-cxxsupp
  yutil
  filestore-public-api-protos
  contrib-libs-protobuf
)
target_proto_messages(libs-vfs_fuse-protos PRIVATE
  ${CMAKE_SOURCE_DIR}/cloud/filestore/libs/vfs_fuse/protos/queue_entry.proto
)
target_proto_addincls(libs-vfs_fuse-protos
  ./
  ${CMAKE_SOURCE_DIR}/
  ${CMAKE_BINARY_DIR}
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf/src
  ${CMAKE_BINARY_DIR}
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf/src
)
target_proto_outs(libs-vfs_fuse-protos
  --cpp_out=${CMAKE_BINARY_DIR}/
  --cpp_styleguide_out=${CMAKE_BINARY_DIR}/
)
