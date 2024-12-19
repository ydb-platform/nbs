#include "xslt_render.h"

#include <library/cpp/testing/unittest/registar.h>

namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NCloud;

TString ContentAddChildren(const auto& name, const auto& value)
{
    TStringStream out;
    out << "<root><cd><name>" << name << "</name><value>" << value
        << "</value></cd></root>";
    return out.Str();
}

void TestAddChildren(const auto& name, const auto& value)
{
    TXmlNodeWrapper wrapper("root", TXmlNodeWrapper::ESource::ROOT_NAME);
    wrapper.AddNamedElement(name, value);
    UNIT_ASSERT_VALUES_EQUAL(
        ContentAddChildren(name, value),
        wrapper.ToString());
}

TString ContentAddChild(const auto& name, const auto& value)
{
    TStringStream out;
    out << "<" << name << ">" << value << "</" << name << ">";
    return out.Str();
}

TXmlNodeWrapper TestAddChild(const auto& name, const auto& value)
{
    TXmlNodeWrapper wrapper("root", TXmlNodeWrapper::ESource::ROOT_NAME);
    auto child = wrapper.AddChild(name, value);
    UNIT_ASSERT_VALUES_EQUAL(ContentAddChildren(name, value), child.ToString());
    return child;
}

}   // namespace

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TXmlNodeWrapperTest)
{
    Y_UNIT_TEST(ShouldAddChild)
    {
        TestAddChildren("a", "b");
        TestAddChildren(1, 1);
        TestAddChildren(1e-2, 1e-2);
    }

    Y_UNIT_TEST(ShouldAddElemWithNameAndValue)
    {
        TestAddChildren("a", "b");
        TestAddChildren(1, 1);
        TestAddChildren(1e-2, 1e-2);
    }
}

}   // namespace NCloud
