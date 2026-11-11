PROGRAM(fastshard-loadtest)

# Per-thread caches, not per-cpu: tcmalloc's per-cpu mode registers rseq
# for every thread and, on glibc < 2.35 (which does not register rseq
# itself), silk's own registration then fails with EINVAL.
ALLOCATOR(TCMALLOC_TC)

SRCS(
    app.cpp
    bootstrap.cpp
    main.cpp
    options.cpp
)

PEERDIR(
    cloud/fastshard/apps/loadtest/lib

    cloud/fastshard/libs/bootstrap
    cloud/fastshard/libs/sn/client
    cloud/fastshard/libs/sn/iface

    library/cpp/getopt
    library/cpp/protobuf/json

    contrib/libs/silk/src/fibers
)

END()
