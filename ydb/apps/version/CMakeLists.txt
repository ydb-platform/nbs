
add_library(apps-version)

target_link_libraries(apps-version PUBLIC
  contrib-libs-linux-headers
  contrib-libs-cxxsupp
  yutil
  ydb-core-driver_lib-version
)

target_sources(apps-version PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/apps/version/version_definition.cpp
)
