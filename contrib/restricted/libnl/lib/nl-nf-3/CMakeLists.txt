add_library(contrib-restricted-libnl-lib-nl-nf-3)

target_link_libraries(contrib-restricted-libnl-lib-nl-nf-3 PRIVATE
  contrib-restricted-libnl-lib-nl-3
)

target_include_directories(contrib-restricted-libnl-lib-nl-nf-3 PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/include
)

target_compile_options(contrib-restricted-libnl-lib-nl-nf-3 PRIVATE
  -DHAVE_CONFIG_H
  -DSYSCONFDIR=\"/etc/libnl\"
)

target_sources(contrib-restricted-libnl-lib-nl-nf-3 PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/netfilter/ct.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/netfilter/ct_obj.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/netfilter/exp.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/netfilter/exp_obj.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/netfilter/log.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/netfilter/log_msg.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/netfilter/log_msg_obj.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/netfilter/log_obj.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/netfilter/netfilter.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/netfilter/nfnl.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/netfilter/queue.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/netfilter/queue_msg.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/netfilter/queue_msg_obj.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/libnl/lib/netfilter/queue_obj.c
)
