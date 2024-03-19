
add_library(core-libs-version_ydb)

target_link_libraries(version PUBLIC
  contrib-libs-linux-headers
  contrib-libs-cxxsupp
  yutil
  library-cpp-svnversion
  ydb-core-protos
  version
)

target_sources(core-libs-version_ydb PRIVATE
  ${CMAKE_SOURCE_DIR}/cloud/storage/core/libs/version_ydb/version.cpp
)
