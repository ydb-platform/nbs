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
        auto error = renderer.Render(document, result);
        UNIT_ASSERT_VALUES_EQUAL(0, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            NResource::Find("xslt_render/ut/result1"),
            result.Str());
    }

    Y_UNIT_TEST(ShouldSupportMultithreading)
    {
        TString data = NResource::Find("xslt_render/ut/xml1");
        std::vector<std::shared_ptr<std::thread>> threads;
        for (size_t i = 0; i < 1000; i++) {
            threads.push_back(std::make_shared<std::thread>(
                [&]()
                {
                    TXslRenderer renderer(
                        NResource::Find("xslt_render/ut/style1").c_str());
                    TXmlNodeWrapper document(
                        data,
                        TXmlNodeWrapper::ESource::STRING);
                    TStringStream result;
                    auto error = renderer.Render(document, result);
                    UNIT_ASSERT_VALUES_EQUAL(0, error.GetCode());
                    UNIT_ASSERT_VALUES_EQUAL(
                        NResource::Find("xslt_render/ut/result1"),
                        result.Str());
                }));
        }
        for (auto& thread: threads) {
            thread->join();
        }
    }
}

}   // namespace NCloud
