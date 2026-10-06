IF (NOT OS_LINUX OR NOT ARCH_X86_64)
    MESSAGE(FATAL_ERROR "AOCL LibMem is supported on Linux x86-64")
ENDIF()

NO_COMPILER_WARNINGS()

# Do not turn implementation loops into calls to the libc functions we replace.
CFLAGS(
    -O3
    -fno-builtin
    -fno-common
    -falign-functions=64
    -falign-loops=64
)

ADDINCL(
    contrib/libs/aocl-libmem/include
)
