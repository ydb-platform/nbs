LIBRARY()

WITHOUT_LICENSE_TEXTS()

LICENSE(MIT)

VERSION(2024)

NO_COMPILER_WARNINGS()

ADDINCL(
    GLOBAL contrib/libs/rseq/include
)

SRCS(
    src/rseq.c
    src/smp.c
)

END()
