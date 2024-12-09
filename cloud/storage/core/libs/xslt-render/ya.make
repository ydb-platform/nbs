LIBRARY(xslt-render)

SRCS(
    xml-document.cpp
    xslt-render.cpp
)

PEERDIR(
    library/cpp/xml/document
    contrib/libs/libxslt
)

END()

RECURSE_FOR_TESTS(
    ut
)
