
add_library(apps-version)

target_link_libraries(version PUBLIC
  contrib-libs-linux-headers
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
  cpp-actors-interconnect
  library-cpp-svnversion
  ydb-core-protos
)

target_sources(apps-version PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/apps/version/version_definition.cpp
)
