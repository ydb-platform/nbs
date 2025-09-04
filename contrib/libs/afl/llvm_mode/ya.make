LIBRARY()

VERSION(2.35b)

LICENSE(
    Apache-2.0 AND
    Public-Domain
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

NO_COMPILER_WARNINGS()

NO_SANITIZE()

NO_SANITIZE_COVERAGE()

SRCS(
    afl-llvm-rt.o.c
)

END()
