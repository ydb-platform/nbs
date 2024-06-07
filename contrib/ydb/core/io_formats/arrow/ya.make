RECURSE_FOR_TESTS(ut)

LIBRARY()

SRCS(
    csv_arrow.cpp
)

CFLAGS(
    -Wno-unused-parameter
)

PEERDIR(
    contrib/ydb/core/scheme_types
    contrib/ydb/core/formats/arrow
)

YQL_LAST_ABI_VERSION()

END()
