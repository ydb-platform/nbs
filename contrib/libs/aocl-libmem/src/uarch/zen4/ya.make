LIBRARY()

LICENSE(BSD-3-Clause)
LICENSE_TEXTS(../../../LICENSE.txt)

INCLUDE(${ARCADIA_ROOT}/contrib/libs/aocl-libmem/build.inc)

# Match upstream ALMEM_ARCH=zen4; runtime CPU dispatch is disabled.
CFLAGS(
    -march=znver4
    -mno-vzeroupper
)

# Keep the weak libc aliases in the executable so they replace libc calls.
# memcpy_impl_zen4.c and ISA implementations are included by these sources.
GLOBAL_SRCS(
    memcpy_zen4.c
    mempcpy_zen4.c
    memmove_zen4.c
    memset_zen4.c
    memcmp_zen4.c
    memchr_zen4.c
    strcpy_zen4.c
    strncpy_zen4.c
    strcmp_zen4.c
    strncmp_zen4.c
    strcat_zen4.c
    strncat_zen4.c
    strstr_zen4.c
    strlen_zen4.c
    strnlen_zen4.c
    strchr_zen4.c
    strrchr_zen4.c
    strspn_zen4.c
)

END()
