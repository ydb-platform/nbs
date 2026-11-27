#include "proto_helpers.h"

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TProtoHelpersTest)
{
    Y_UNIT_TEST(ShouldNotMakeResponseOfUnexpectedPageCount)
    {
        TVector<TBuffer> pages;
        pages.emplace_back("a", 1);

        const auto response =
            MakeReadPagesResponse({{.FirstPageNo = 10, .PageCount = 2}}, pages);

        UNIT_ASSERT_VALUES_EQUAL_C(
            E_INVALID_STATE,
            response.GetError().GetCode(),
            FormatError(response.GetError()));
    }

    Y_UNIT_TEST(ShouldSetLastAckedLsnInReadPagesResponse)
    {
        TReadPagesResult result{.LastAckedLsn = 42};
        result.Pages.emplace_back("a", 1);
        result.Pages.emplace_back("b", 1);

        const auto response = MakeReadPagesResponse(
            {{.FirstPageNo = 10, .PageCount = 2}},
            result);

        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response.GetError().GetCode(),
            FormatError(response.GetError()));
        UNIT_ASSERT_VALUES_EQUAL(42, response.GetLastAckedLogSequenceNumber());
        UNIT_ASSERT_VALUES_EQUAL(1, response.PageGroupsSize());
        UNIT_ASSERT_VALUES_EQUAL(
            10,
            response.GetPageGroups(0).GetFirstPageNo());
        UNIT_ASSERT_VALUES_EQUAL("a", response.GetPageGroups(0).GetContent(0));
        UNIT_ASSERT_VALUES_EQUAL("b", response.GetPageGroups(0).GetContent(1));
    }
}

}   // namespace NCloud::NJournalled
