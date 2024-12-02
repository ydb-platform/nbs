LIBRARY(xsl-render)

SRCS(
    xml_document.cpp
    xsl_render.cpp
)

PEERDIR(
    library/cpp/xml/document
    contrib/libs/libxslt
)

END()
