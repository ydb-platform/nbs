UNITTEST_FOR(cloud/storage/core/libs/xslt_render)

SRCS(
    xml_document_ut.cpp
    xslt_render_ut.cpp
)

PEERDIR (
    library/cpp/resource
    library/cpp/testing/unittest
)

RESOURCE(
    ut/test_cases/1.xsl     xslt_render/ut/style1
    ut/test_cases/1.xml     xslt_render/ut/xml1
    ut/test_cases/1.result  xslt_render/ut/result1
)

END()
