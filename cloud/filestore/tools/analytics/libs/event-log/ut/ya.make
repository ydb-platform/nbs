UNITTEST_FOR(cloud/filestore/tools/analytics/libs/event-log)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/small.inc)

SRCS(
    dump_ut.cpp
    request_filter_ut.cpp
    request_printer_ut.cpp
)

END()
