add_library(contrib-restricted-libnl-lib-nl-idiag-3)

target_link_libraries(contrib-restricted-libnl-lib-nl-idiag-3 PRIVATE
  contrib-restricted-libnl-lib-nl-3
)

target_include_directories(contrib-restricted-libnl-lib-nl-idiag-3 PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/include
)

target_compile_options(contrib-restricted-libnl-lib-nl-idiag-3 PRIVATE
  -DHAVE_CONFIG_H
  -DSYSCONFDIR=\"/etc/libnl\"
)

target_sources(contrib-restricted-libnl-lib-nl-idiag-3 PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/idiag/idiag.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/idiag/idiag_meminfo_obj.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/idiag/idiag_msg_obj.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/idiag/idiag_req_obj.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/idiag/idiag_vegasinfo_obj.c
)
