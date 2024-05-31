add_subdirectory(ut)

add_library(libs-endpoint_proxy-client)
target_link_libraries(libs-endpoint_proxy-client PUBLIC
  contrib-libs-linux-headers
  contrib-libs-cxxsupp
  yutil
  public-api-grpc
  public-api-protos
  core-libs-common
  core-libs-diagnostics
  storage-core-protos
  contrib-libs-grpc
)
target_sources(libs-endpoint_proxy-client PRIVATE
  ${CMAKE_SOURCE_DIR}/cloud/blockstore/libs/endpoint_proxy/client/client.cpp
  ${CMAKE_SOURCE_DIR}/cloud/blockstore/libs/endpoint_proxy/client/device_factory.cpp
)
