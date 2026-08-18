#include "simple_template.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/str.h>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString Render(
    const TString& templateData,
    const TTemplateVars& vars,
    const TTemplateArrays& arrays = {})
{
    TStringStream out;
    OutputTemplate(templateData, vars, arrays, out);
    return out.Str();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSimpleTemplateTest)
{
    Y_UNIT_TEST(ShouldSubstituteVars)
    {
        UNIT_ASSERT_VALUES_EQUAL(
            "a=1 b=2",
            Render("a={{ a }} b={{ b }}", {{"a", "1"}, {"b", "2"}}));
    }

    Y_UNIT_TEST(ShouldDropUnknownVars)
    {
        UNIT_ASSERT_VALUES_EQUAL("x=", Render("x={{ x }}", {}));
    }

    Y_UNIT_TEST(ShouldCopyDanglingTokenVerbatim)
    {
        UNIT_ASSERT_VALUES_EQUAL("x={{ x", Render("x={{ x", {{"x", "1"}}));
    }

    Y_UNIT_TEST(ShouldRenderLoops)
    {
        const TTemplateArrays arrays = {
            {"rows", {{{"v", "1"}}, {{"v", "2"}}, {{"v", "3"}}}},
        };

        UNIT_ASSERT_VALUES_EQUAL(
            "[1][2][3]",
            Render("{{ for rows }}[{{ v }}]{{ endfor }}", {}, arrays));
    }

    Y_UNIT_TEST(ShouldRenderEmptyAndUnknownLoops)
    {
        const TTemplateArrays arrays = {
            {"empty", {}},
        };

        UNIT_ASSERT_VALUES_EQUAL(
            "<>",
            Render("<{{ for empty }}x{{ endfor }}>", {}, arrays));
        UNIT_ASSERT_VALUES_EQUAL(
            "<>",
            Render("<{{ for unknown }}x{{ endfor }}>", {}, arrays));
    }

    Y_UNIT_TEST(ShouldShadowOuterVarsInsideLoops)
    {
        const TTemplateArrays arrays = {
            {"rows", {{{"v", "inner"}}, {}}},
        };

        UNIT_ASSERT_VALUES_EQUAL(
            "[inner][outer]",
            Render(
                "{{ for rows }}[{{ v }}]{{ endfor }}",
                {{"v", "outer"}},
                arrays));
    }

    Y_UNIT_TEST(ShouldRenderNestedLoops)
    {
        const TTemplateArrays arrays = {
            {"outer", {{{"o", "A"}}, {{"o", "B"}}}},
            {"inner", {{{"i", "1"}}, {{"i", "2"}}}},
        };

        UNIT_ASSERT_VALUES_EQUAL(
            "A(A1)(A2)B(B1)(B2)",
            Render(
                "{{ for outer }}{{ o }}"
                "{{ for inner }}({{ o }}{{ i }}){{ endfor }}"
                "{{ endfor }}",
                {},
                arrays));
    }

    Y_UNIT_TEST(ShouldRenderNothingForUnterminatedLoop)
    {
        UNIT_ASSERT_VALUES_EQUAL(
            "<x",
            Render("<{{ for rows }}x", {}, {{"rows", {{}}}}));
    }
}

}   // namespace NCloud
