#include "xslt-render.h"

#include <library/cpp/resource/resource.h>
#include <library/cpp/testing/unittest/registar.h>

#include <thread>

namespace NCloud::NXslRender {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(XsltRenderTest)
{
    Y_UNIT_TEST(ShouldRenderXslt)
    {
        TXslRenderer renderer(NResource::Find("xslt-render/ut/style1").c_str());
        NXml::TDocument document(
            NResource::Find("xslt-render/ut/xml1"),
            NXml::TDocument::String);
        TStringStream result;
        renderer.Render(document, result);
        UNIT_ASSERT_VALUES_EQUAL(
            result.Str(),
            NResource::Find("xslt-render/ut/result1"));
    }

    Y_UNIT_TEST(ShouldSupportMultithreading)
    {
        TXslRenderer renderer(NResource::Find("xslt-render/ut/style1").c_str());
        TString data = NResource::Find("xslt-render/ut/xml1");
        TString resultStr = NResource::Find("xslt-render/ut/result1");
        std::vector<std::shared_ptr<std::thread>> threads;
        for (size_t i = 0; i < 1000; i++) {
            threads.push_back(std::make_shared<std::thread>(
                [&]()
                {
                    NXml::TDocument document(data, NXml::TDocument::String);
                    TStringStream result;
                    renderer.Render(document, result);
                    UNIT_ASSERT_VALUES_EQUAL(result.Str(), resultStr);
                }));
        }
        for (auto& thread: threads) {
            thread->join();
        }
    }
}

}   // namespace NCloud::NXslRender
