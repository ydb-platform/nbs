add_subdirectory(ut)

add_library(core-libs-endpoints-fs)
target_link_libraries(core-libs-endpoints-fs PUBLIC
  contrib-libs-linux-headers
  contrib-libs-cxxsupp
  yutil
  core-libs-common
)
target_sources(core-libs-endpoints-fs PRIVATE
  ${CMAKE_SOURCE_DIR}/cloud/storage/core/libs/endpoints/fs/fs_endpoints.cpp
)
