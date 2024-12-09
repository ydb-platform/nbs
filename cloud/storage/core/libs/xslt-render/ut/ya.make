UNITTEST_FOR(cloud/storage/core/libs/xslt-render)

SRCS(
    ut-xml-document.cpp
    ut-xslt-render.cpp
)

PEERDIR (
    library/cpp/resource
    library/cpp/testing/unittest
)

RESOURCE(
    ut/test_cases/1.xsl     xslt-render/ut/style1
    ut/test_cases/1.xml     xslt-render/ut/xml1
    ut/test_cases/1.result  xslt-render/ut/result1
)

END()
