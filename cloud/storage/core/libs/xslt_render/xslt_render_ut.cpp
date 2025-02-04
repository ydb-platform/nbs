#include "xslt_render.h"

#include <library/cpp/resource/resource.h>
#include <library/cpp/testing/unittest/registar.h>

#include <thread>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TXsltRenderTest)
{
    Y_UNIT_TEST(ShouldRenderXslt)
    {
        TXslRenderer renderer(NResource::Find("xslt_render/ut/style1").c_str());
        TXmlNodeWrapper document(
            NResource::Find("xslt_render/ut/xml1"),
            TXmlNodeWrapper::ESource::STRING);
        TStringStream result;
        int resultCode = renderer.Render(document, result);
        UNIT_ASSERT_VALUES_EQUAL(resultCode, 0);
        UNIT_ASSERT_VALUES_EQUAL(
            NResource::Find("xslt_render/ut/result1"),
            result.Str());
    }

    Y_UNIT_TEST(ShouldSupportMultithreading)
    {
        TXslRenderer renderer(NResource::Find("xslt_render/ut/style1").c_str());
        TString data = NResource::Find("xslt_render/ut/xml1");
        TString resultStr = NResource::Find("xslt_render/ut/result1");
        std::vector<std::shared_ptr<std::thread>> threads;
        for (size_t i = 0; i < 1000; i++) {
            threads.push_back(std::make_shared<std::thread>(
                [&]()
                {
                    TXmlNodeWrapper document(
                        data,
                        TXmlNodeWrapper::ESource::STRING);
                    TStringStream result;
                    int resultCode = renderer.Render(document, result);
                    UNIT_ASSERT_VALUES_EQUAL(resultCode, 0);
                    UNIT_ASSERT_VALUES_EQUAL(resultStr, result.Str());
                }));
        }
        for (auto& thread: threads) {
            thread->join();
        }
    }
}

}   // namespace NCloud
