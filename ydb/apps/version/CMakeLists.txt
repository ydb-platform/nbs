
add_library(apps-version)

target_link_libraries(version PUBLIC
  ydb-core-driver_lib-version
  contrib-libs-linux-headers
  contrib-libs-cxxsupp
  yutil
  library-cpp-svnversion
)

target_sources(apps-version PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/apps/version/version_definition.cpp
)
