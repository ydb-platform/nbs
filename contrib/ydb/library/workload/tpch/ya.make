LIBRARY()

SRCS(
    driver.c
    driver.cpp
    GLOBAL data_generator.cpp
    GLOBAL registrar.cpp
    tpch.cpp
)

IF (OS_MACOS OR OS_DARWIN)
    CFLAGS(-DLINUX)
ELSEIF (OS_WINDOWS)
    CONLYFLAGS(-DWIN32)
    CXXFLAGS(-D_POSIX_ -DLINUX)
ELSEIF (OS_LINUX)
    CONLYFLAGS(-D_POSIX_SOURCE)
    CFLAGS(-DLINUX)
ENDIF()

RESOURCE(
    tpch_schema.yaml tpch_schema.yaml
    contrib/ydb/library/benchmarks/gen/tpch-dbgen/dists.dss dists.dss
)

ALL_RESOURCE_FILES_FROM_DIRS(
    PREFIX tpch/
    s0.1_canonical
    s0.2_canonical
    s0.5_canonical
    s1_canonical
    s10_canonical
    s100_canonical
    s1000_canonical
    s10000_canonical
)

PEERDIR(
    contrib/libs/fmt
    library/cpp/resource
    contrib/ydb/library/accessor
    contrib/ydb/library/benchmarks/gen/tpch-dbgen
    contrib/ydb/library/benchmarks/queries/tpch
    contrib/ydb/library/workload/tpc_base
)

END()

RECURSE_FOR_TESTS(ut)
