add_library(contrib-restricted-libnl-lib-nl-3)

target_include_directories(contrib-restricted-libnl-lib-nl-3 PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/include
)

target_compile_options(contrib-restricted-libnl-lib-nl-3 PRIVATE
  -DHAVE_CONFIG_H
  -DSYSCONFDIR=\"/etc/libnl\"
)

target_sources(contrib-restricted-libnl-lib-nl-3 PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/addr.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/attr.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/cache.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/cache_mngr.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/cache_mngt.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/data.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/error.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/handlers.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/hash.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/hashtable.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/mpls.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/msg.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/nl.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/object.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/socket.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/utils.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/version.c
)
