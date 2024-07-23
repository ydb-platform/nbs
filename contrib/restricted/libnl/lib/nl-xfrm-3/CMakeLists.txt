add_library(contrib-restricted-libnl-lib-nl-xfrm-3)

target_link_libraries(contrib-restricted-libnl-lib-nl-xfrm-3 PUBLIC
  contrib-libs-linux-headers
  contrib-restricted-libnl-lib-nl-3
)

target_include_directories(contrib-restricted-libnl-lib-nl-xfrm-3 PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/include
)

target_compile_options(contrib-restricted-libnl-lib-nl-xfrm-3 PRIVATE
    -DHAVE_CONFIG_H
    -DSYSCONFDIR=\"/etc/libnl\"
)

target_sources(contrib-restricted-libnl-lib-nl-xfrm-3 PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/xfrm/ae.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/xfrm/lifetime.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/xfrm/sa.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/xfrm/selector.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/xfrm/sp.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/xfrm/template.c
)
