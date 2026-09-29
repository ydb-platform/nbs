LIBRARY(filestore-apps-client-query)

SRCS(
    lexer_generated.cpp
    parser.cpp
    parser_generated.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
)

END()

RECURSE_FOR_TESTS(
    ut
)
