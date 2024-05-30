add_subdirectory(ut)

add_library(libs-endpoint_proxy-server)
target_link_libraries(libs-endpoint_proxy-server PUBLIC
  contrib-libs-linux-headers
  contrib-libs-cxxsupp
  yutil
  public-api-grpc
  public-api-protos
  libs-daemon-common
  blockstore-libs-nbd
  blockstore-libs-service
  core-libs-common
  core-libs-diagnostics
  core-libs-grpc
  core-libs-uds

  cpp-actors-util
  cpp-getopt-small
  library-cpp-logger

  storage-core-protos
  contrib-libs-grpc
)
target_sources(libs-endpoint_proxy-server PRIVATE
  ${CMAKE_SOURCE_DIR}/cloud/blockstore/libs/endpoint_proxy/server/bootstrap.cpp
  ${CMAKE_SOURCE_DIR}/cloud/blockstore/libs/endpoint_proxy/server/options.cpp
  ${CMAKE_SOURCE_DIR}/cloud/blockstore/libs/endpoint_proxy/server/server.cpp
)
