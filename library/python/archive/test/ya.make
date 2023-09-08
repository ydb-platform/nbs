PY23_TEST()

STYLE_PYTHON()

TEST_SRCS(
    test_archive.py
)

PEERDIR(
    contrib/python/six
    library/python/archive
)

DEPENDS(
    library/python/archive/test/data
)

END()
