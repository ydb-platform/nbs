Y_BENCHMARK(smootherstep-bench)

IF (SANITIZER_TYPE)
    TAG(ya:manual)
ENDIF()

ALLOCATOR(TCMALLOC_TC)

SRCS(
    main.cpp
)

END()
