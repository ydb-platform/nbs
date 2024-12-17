#include "xslt_render.h"

#include <library/cpp/testing/unittest/registar.h>

namespace {

////////////////////////////////////////////////////////////////////////////////

TString Content(auto&& name, auto&& value)
{
    TStringStream out;
    out << "<root><cd><name>" << std::forward<decltype(name)>(name)
        << "</name><value>" << std::forward<decltype(value)>(value)
        << "</value></cd></root>";
    return out.Str();
}

void TestAddChildren(auto&& name, auto&& value)
{
    NXml::TDocument data("root", NXml::TDocument::RootName);
    auto root = data.Root();
    NCloud::TXmlNodeWrapper wrapper(root);
    wrapper.AddNamedElement(name, value);
    UNIT_ASSERT_VALUES_EQUAL(Content(name, value), root.ToString());
}

}   // namespace

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TXmlNodeWrapperTest)
{
    Y_UNIT_TEST(ShouldAddElemWithNameAndValue)
    {
        TestAddChildren("a", "b");
        TestAddChildren(1, 1);
        TestAddChildren(1e-2, 1e-2);
    }
}

}   // namespace NCloud
