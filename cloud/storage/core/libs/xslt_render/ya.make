LIBRARY(xslt_render)

SRCS(
    xml_document.cpp
    xslt_render.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
    library/cpp/xml/document
    contrib/libs/libxslt
)

END()

RECURSE_FOR_TESTS(
    ut
)
