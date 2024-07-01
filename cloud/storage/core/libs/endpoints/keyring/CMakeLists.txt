add_subdirectory(ut)

add_library(core-libs-endpoints-keyring)
target_link_libraries(core-libs-endpoints-keyring PUBLIC
  contrib-libs-linux-headers
  contrib-libs-cxxsupp
  yutil
  core-libs-common
)
target_sources(core-libs-endpoints-keyring PRIVATE
  ${CMAKE_SOURCE_DIR}/cloud/storage/core/libs/endpoints/keyring/keyring_endpoints.cpp
  ${CMAKE_SOURCE_DIR}/cloud/storage/core/libs/endpoints/keyring/keyring_endpoints_test.cpp
  ${CMAKE_SOURCE_DIR}/cloud/storage/core/libs/endpoints/keyring/keyring.cpp
)
