LIBRARY()

SRCS(
    version.cpp
)

PEERDIR(
    library/cpp/svnversion
)

END()

RECURSE_FOR_TESTS(ut)
