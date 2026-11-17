#include "device.h"

#include <cloud/fastshard/libs/journal/impl/memory_device.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>
#include <util/generic/string.h>

namespace NCloud::NJournalled {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 PageSize = 4_KB;

TPageRange MakeRange(ui64 firstPageNo, char fill)
{
    TPageRange range;
    range.FirstPageNo = firstPageNo;
    range.Pages.emplace_back(TString(PageSize, fill).data(), PageSize);
    return range;
}

TString ReadPage(const IDevicePtr& device, ui64 pageNo)
{
    auto result = device->ReadPages({{pageNo, 1}}).GetValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(
        S_OK,
        result.GetError().GetCode(),
        FormatError(result.GetError()));

    const auto& page = result.GetResult()[0];
    return TString(page.Data(), page.Size());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRegionDeviceTest)
{
    Y_UNIT_TEST(ShouldAddressRegionFromItsFirstPage)
    {
        auto whole = CreateInMemoryDevice(PageSize);
        auto region = CreateRegionDevice(whole, {4, 8});

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            region->WritePages({MakeRange(0, 'a'), MakeRange(7, 'b')})
                .GetValueSync()
                .GetCode());

        UNIT_ASSERT_VALUES_EQUAL(TString(PageSize, 'a'), ReadPage(whole, 4));
        UNIT_ASSERT_VALUES_EQUAL(TString(PageSize, 'b'), ReadPage(whole, 11));
        UNIT_ASSERT_VALUES_EQUAL(TString(PageSize, 'a'), ReadPage(region, 0));
        UNIT_ASSERT_VALUES_EQUAL(TString(PageSize, 'b'), ReadPage(region, 7));

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            region->ZeroPages({{0, 8}}).GetValueSync().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(TString(PageSize, '\0'), ReadPage(whole, 4));
        UNIT_ASSERT_VALUES_EQUAL(TString(PageSize, '\0'), ReadPage(whole, 11));
    }

    Y_UNIT_TEST(ShouldRejectPagesOutsideRegion)
    {
        auto region =
            CreateRegionDevice(CreateInMemoryDevice(PageSize), {4, 8});

        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            region->ReadPages({{7, 2}}).GetValueSync().GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            region->WritePages({MakeRange(8, 'a')}).GetValueSync().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            region->ZeroPages({{0, 9}}).GetValueSync().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            region->ZeroPages({{Max<ui64>(), 1}}).GetValueSync().GetCode());
    }
}

}   // namespace NCloud::NJournalled
