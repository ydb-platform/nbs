OWNER(g:cloud-nbs)

LIBRARY()

SRCS(
    version.cpp
)

PEERDIR(
    contrib/ydb/core/driver_lib/version
)

END()

RECURSE_FOR_TESTS(ut)

