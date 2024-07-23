add_library(contrib-restricted-libnl-lib-nl-genl-3)

target_link_libraries(contrib-restricted-libnl-lib-nl-genl-3 PRIVATE
  contrib-restricted-libnl-lib-nl-3
)

target_include_directories(contrib-restricted-libnl-lib-nl-genl-3 PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/include
)

target_compile_options(contrib-restricted-libnl-lib-nl-genl-3 PRIVATE
  -DHAVE_CONFIG_H
  -DSYSCONFDIR=\"/etc/libnl\"
)

target_sources(contrib-restricted-libnl-lib-nl-genl-3 PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/genl/ctrl.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/genl/family.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/genl/genl.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/genl/mngt.c
)
