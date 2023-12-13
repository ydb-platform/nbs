PY23_TEST()

STYLE_PYTHON()

TEST_SRCS(
    compare.py
)

SIZE(LARGE)

TAG(ya:fat)

PEERDIR(
    library/python/archive
)

DEPENDS(
    library/python/archive/benchmark/data
)

END()
