#include "xslt_render.h"

#include <library/cpp/testing/unittest/registar.h>

namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NCloud;

TString ContentAddNamedElement(const auto& name, const auto& value)
{
    TStringStream out;
    out << "<root><item><name>" << name << "</name><value>" << value
        << "</value></item></root>";
    return out.Str();
}

void TestAddNamedElement(const auto& name, const auto& value)
{
    TXmlNodeWrapper wrapper("root", TXmlNodeWrapper::ESource::ROOT_NAME);
    wrapper.AddNamedElement(name, value);
    UNIT_ASSERT_VALUES_EQUAL(
        ContentAddNamedElement(name, value),
        wrapper.ToString());
}

TString ContentAddChild(const auto& tag, const auto& content)
{
    TStringStream out;
    out << "<" << tag << ">" << content << "</" << tag << ">";
    return out.Str();
}

void TestAddChild(const auto& tag, const auto& content)
{
    TXmlNodeWrapper wrapper("root", TXmlNodeWrapper::ESource::ROOT_NAME);
    auto child = wrapper.AddChild(tag, content);
    UNIT_ASSERT_VALUES_EQUAL(ContentAddChild(tag, content), child.ToString());
}

}   // namespace

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TXmlNodeWrapperTest)
{
    Y_UNIT_TEST(ShouldAddChild)
    {
        TestAddChild("a", "b");
        TestAddChild(1, 1);
        TestAddChild(1e-2, 1e-2);
    }

    Y_UNIT_TEST(ShouldAddElemWithNameAndValue)
    {
        TestAddNamedElement("a", "b");
        TestAddNamedElement(1, 1);
        TestAddNamedElement(1e-2, 1e-2);
    }
}

}   // namespace NCloud
