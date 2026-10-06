LIBRARY()

VERSION(5.3.2)
LICENSE(BSD-3-Clause)
LICENSE_TEXTS(LICENSE.txt)

INCLUDE(build.inc)

# libmem.c includes the CPU detection, cache and threshold implementations.
# Retain the constructor even when no AOCL-specific symbol is referenced.
GLOBAL_SRCS(
    src/libmem.c
)

PEERDIR(
    contrib/libs/aocl-libmem/src/uarch/zen4
)

END()
