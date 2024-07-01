add_library(core-libs-endpoints-iface INTERFACE)

target_include_directories(core-libs-endpoints-iface INTERFACE
  ${CMAKE_SOURCE_DIR}/cloud/storage/core/libs/endpoints/iface
)
