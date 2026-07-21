LIBRARY()

SRCS(
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/reader/trivial_reader/constructor
    contrib/ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator
)

END()

RECURSE_FOR_TESTS(
    duplicates
)
