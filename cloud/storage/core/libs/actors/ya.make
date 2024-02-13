LIBRARY()

SRCS(
    helpers.cpp
    poison_pill_helper.cpp
)

PEERDIR(
    library/cpp/actors/core
)

END()

RECURSE_FOR_TESTS(ut)
