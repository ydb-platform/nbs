add_library(core-libs-http)

target_link_libraries(core-libs-http PUBLIC
  cpp-http-server
)

target_sources(core-libs-http PRIVATE
  ${CMAKE_SOURCE_DIR}/cloud/storage/core/libs/http/simple_http_server.cpp
)
