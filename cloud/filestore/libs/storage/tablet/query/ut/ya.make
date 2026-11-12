UNITTEST_FOR(cloud/filestore/libs/storage/tablet/query)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/small.inc)

SRCS(
    executor_ut.cpp
    query_ut.cpp
)

END()
