add_executable(blockstore-endpoint-proxy)
target_compile_options(blockstore-endpoint-proxy PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(blockstore-endpoint-proxy PUBLIC
  contrib-libs-linux-headers
  contrib-libs-cxxsupp
  yutil
  library-cpp-cpuid_check
  libs-endpoint_proxy-server
  core-libs-daemon
  library-cpp-getopt
)
target_link_options(blockstore-endpoint-proxy PRIVATE
  -ldl
  -lrt
  -Wl,--no-as-needed
  -fPIC
  -fPIC
  -lpthread
  -lrt
  -ldl
)
target_sources(blockstore-endpoint-proxy PRIVATE
  ${CMAKE_SOURCE_DIR}/cloud/blockstore/apps/endpoint_proxy/main.cpp
)
target_allocator(blockstore-endpoint-proxy
  cpp-malloc-tcmalloc
  libs-tcmalloc-no_percpu_cache
)
vcs_info(blockstore-endpoint-proxy)
