#include <library/cpp/testing/unittest/registar.h>

#include "xslt-render.h"

namespace {

TString Content(auto && name, auto && value) {
    TStringStream out;
    out << "<root><cd><name>"
        << std::forward<decltype(name)>(name)
        << "</name><value>"
        << std::forward<decltype(value)>(value)
        << "</value></cd></root>";
    return out.Str();
}

void TestAddChilds(auto && name, auto && value) {
    using namespace NCloud::NStorage::NTNodeWrapper;

    NXml::TDocument data("root", NXml::TDocument::RootName);
    auto root = data.Root();
    TNodeWrapper wrapper(root);
    wrapper.AddNamedElement(name, value);
    UNIT_ASSERT_VALUES_EQUAL(root.ToString(), Content(name, value));
}

}  // namespace

namespace NCloud::NStorage::NTNodeWrapper {

///////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(NodeWrapperTest) {
    Y_UNIT_TEST(ShouldAddElemWithNameAndValue) {
        TestAddChilds("a", "b");
        TestAddChilds(1, 1);
        TestAddChilds(1e-2, 1e-2);
    }
}

}  // namespace NCloud::NStorage::NTNodeWrapper