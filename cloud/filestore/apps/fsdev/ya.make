PROGRAM(filestore-fsdev)

IF (PROFILE_MEMORY_ALLOCATIONS)
    ALLOCATOR(LF_DBG)
ELSE()
    ALLOCATOR(TCMALLOC_TC)
ENDIF()

IF (BUILD_TYPE != "PROFILE" AND BUILD_TYPE != "DEBUG")
    SPLIT_DWARF()
ENDIF()

IF (SANITIZER_TYPE)
    NO_SPLIT_DWARF()
ENDIF()

SET(_LD_USE_STDLIB "dasdasd")
LDFLAGS(
    #-L/home/evgenybud/projects/repos/nebius/storage/github.mellanox.spdk/build/lib -L/home/evgenybud/projects/repos/nebius/storage/github.mellanox.spdk/dpdk/build/lib -L/home/evgenybud/projects/repos/nebius/storage/github.mellanox.spdk/build/lib -lspdk_nvme -lspdk_keyring -lspdk_sock -lspdk_sock_posix -lspdk_sock_uring -lspdk_dma -lspdk_vfio_user -lspdk_env_dpdk -lrte_eal -lrte_mempool -lrte_ring -lrte_mbuf -lrte_bus_pci -lrte_pci -lrte_mempool_ring -lrte_telemetry -lrte_kvargs -lrte_rcu -lrte_power -lrte_ethdev -lrte_vhost -lrte_net -lrte_dmadev -lrte_cryptodev -lrte_hash -lrte_log -lspdk_event -lspdk_init -lspdk_thread -lspdk_trace -lspdk_env_dpdk_rpc -lspdk_rpc -lspdk_jsonrpc -lspdk_json -lspdk_util -lspdk_log -L /usr/lib/x86_64-linux-gnu -luuid
    -Wl,--whole-archive -Wl,-Bstatic
    -L/home/evgenybud/projects/repos/nebius/storage/github.mellanox.spdk/build/lib -lspdk_env_dpdk
    -L/home/evgenybud/projects/repos/nebius/storage/github.mellanox.spdk/dpdk/build/lib -lrte_eal -lrte_mempool -lrte_ring -lrte_mbuf -lrte_bus_pci -lrte_pci -lrte_mempool_ring -lrte_telemetry -lrte_kvargs -lrte_rcu -lrte_power -lrte_ethdev -lrte_vhost -lrte_net -lrte_dmadev -lrte_cryptodev -lrte_hash -lrte_log -lspdk_event -lspdk_init -lspdk_thread -lspdk_trace -lspdk_env_dpdk_rpc -lspdk_rpc -lspdk_jsonrpc -lspdk_json -lspdk_util -lspdk_log
    -L /usr/lib/x86_64-linux-gnu -luuid  -lnuma
    #-lgcc
    #/home/evgenybud/.ya/tools/v4/1966560555/usr/lib/x86_64-linux-gnu/libc.a
    #/home/evgenybud/.ya/tools/v4/1966560555/usr/lib/x86_64-linux-gnu/crt1.o
    #/home/evgenybud/.ya/tools/v4/1966560555/usr/lib/x86_64-linux-gnu/crti.o
    #/home/evgenybud/.ya/tools/v4/1966560555/usr/lib/x86_64-linux-gnu/crtn.o
    -Wl,--no-whole-archive -Wl,-Bdynamic
    #-lgcc
    #/home/evgenybud/.ya/tools/v4/1966560555/usr/lib/x86_64-linux-gnu/libc.a
)

SRCS(
    main.cpp
)

ADDINCL(
    cloud/filestore/libs/spdk/lib/include
)

PEERDIR(
    cloud/filestore/libs/spdk/iface
    cloud/filestore/libs/spdk/impl

    cloud/storage/core/libs/common
)


YQL_LAST_ABI_VERSION()

END()
