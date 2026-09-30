#include <cloud/filestore/libs/storage/fastshard/impl/model/format_page.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/bitmap.h>
#include <util/generic/hash_set.h>
#include <util/random/fast.h>

#include <gtest/gtest.h>

using namespace NCloud;
using namespace NFileStore;
using namespace NStorage::NFastShard;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t PageSize = 4_KB;

////////////////////////////////////////////////////////////////////////////////

TVector<ui64> CollectPages(const TVector<TPageGroup>& groups)
{
    TVector<ui64> res;
    for (const auto& pg: groups) {
        for (ui64 i = 0; i < pg.Content.size(); ++i) {
            res.push_back(pg.FirstPageNo + i);
        }
    }

    return res;
}

void Flush(TVector<TPageGroup>& groups, IPageStore& pageStore)
{
    pageStore.CommitPages(CollectPages(groups));
    groups.clear();
}

void Rollback(TVector<TPageGroup>& groups, IPageStore& pageStore)
{
    pageStore.RollbackPages(CollectPages(groups));
    groups.clear();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TEST(FormatPageTest, ChecksVersions)
{
    auto pageStore = CreateMemPageStore(PageSize);

    const ui64 pageNo = 11;
    TFormatPage formatPage;
    formatPage.Init(pageNo, pageStore);

    ui32 minVersion = 1;
    ui32 version = 1;
    TString description = "something";

    {
        TWriteContext writeContext;
        writeContext.Lsn = pageStore->AllocateLsn();
        auto e = formatPage.RegisterStart(
            minVersion,
            version,
            description,
            writeContext);
        EXPECT_EQ(S_OK, e.GetCode()) << FormatError(e);

        Flush(writeContext.PageGroups, *pageStore);
    }

    EXPECT_EQ("G=1 P=11 MV=1 V=1 D={something}", formatPage.Describe());

    ++version;
    description = "something2";

    TFormatPage formatPage2;
    formatPage2.Init(pageNo, pageStore);

    {
        TWriteContext writeContext;
        writeContext.Lsn = pageStore->AllocateLsn();
        auto e = formatPage2.RegisterStart(
            minVersion,
            version,
            description,
            writeContext);
        EXPECT_EQ(S_OK, e.GetCode()) << FormatError(e);

        Flush(writeContext.PageGroups, *pageStore);
    }

    EXPECT_EQ("G=2 P=11 MV=1 V=2 D={something2}", formatPage2.Describe());

    ++version;
    minVersion = version;

    TFormatPage formatPage3;
    formatPage3.Init(pageNo, pageStore);

    {
        TWriteContext writeContext;
        writeContext.Lsn = pageStore->AllocateLsn();
        auto e = formatPage3.RegisterStart(
            minVersion,
            version,
            description,
            writeContext);
        EXPECT_EQ(E_INVALID_STATE, e.GetCode()) << FormatError(e);

        Rollback(writeContext.PageGroups, *pageStore);
    }

    EXPECT_EQ("G=0 P=11 MV=0 V=0 D={}", formatPage3.Describe());
}

TEST(FormatPageTest, ErrorsOnGarbage)
{
    auto pageStore = CreateMemPageStore(PageSize);
    const ui64 pageNo = 11;
    TFormatPage formatPage;
    formatPage.Init(pageNo, pageStore);

    ui32 minVersion = 1;
    ui32 version = 1;
    TString description = "something";

    {
        TBuffer page;
        page.Resize(PageSize);
        memset(page.Data(), 1, PageSize);
        TVector<TPageGroup> pageGroups;
        auto e = pageStore->WritePage(
            pageStore->AllocateLsn(),
            pageNo,
            page,
            pageGroups);
        EXPECT_EQ(S_OK, e.GetCode()) << FormatError(e);

        Flush(pageGroups, *pageStore);
    }

    {
        TWriteContext writeContext;
        writeContext.Lsn = pageStore->AllocateLsn();
        auto e = formatPage.RegisterStart(
            minVersion,
            version,
            description,
            writeContext);
        EXPECT_EQ(E_INVALID_STATE, e.GetCode()) << FormatError(e);

        Rollback(writeContext.PageGroups, *pageStore);
    }

    EXPECT_EQ("G=0 P=11 MV=0 V=0 D={}", formatPage.Describe());
}

TEST(FormatPageTest, ErrorsOnWrongPageNo)
{
    auto pageStore = CreateMemPageStore(PageSize);
    const ui64 pageNo = 11;
    TFormatPage formatPage;
    formatPage.Init(pageNo, pageStore);

    ui32 minVersion = 1;
    ui32 version = 1;
    TString description = "something";

    {
        TWriteContext writeContext;
        writeContext.Lsn = pageStore->AllocateLsn();
        auto e = formatPage.RegisterStart(
            minVersion,
            version,
            description,
            writeContext);
        EXPECT_EQ(S_OK, e.GetCode()) << FormatError(e);

        Flush(writeContext.PageGroups, *pageStore);
    }

    EXPECT_EQ("G=1 P=11 MV=1 V=1 D={something}", formatPage.Describe());

    const ui64 pageNo2 = 21;

    {
        TBuffer originalPage;
        auto e = pageStore->ReadPage(
            pageStore->AllocateLsn(),
            pageNo,
            &originalPage);
        EXPECT_EQ(S_OK, e.GetCode()) << FormatError(e);

        TBuffer page;
        page.Resize(PageSize);
        memcpy(page.Data(), originalPage.Data(), PageSize);
        TVector<TPageGroup> pageGroups;
        e = pageStore->WritePage(
            pageStore->AllocateLsn(),
            pageNo2,
            page,
            pageGroups);
        EXPECT_EQ(S_OK, e.GetCode()) << FormatError(e);

        Flush(pageGroups, *pageStore);
    }

    TFormatPage formatPage2;
    formatPage2.Init(pageNo2, pageStore);

    {
        TWriteContext writeContext;
        writeContext.Lsn = pageStore->AllocateLsn();
        auto e = formatPage2.RegisterStart(
            minVersion,
            version,
            description,
            writeContext);
        EXPECT_EQ(E_INVALID_STATE, e.GetCode()) << FormatError(e);

        Rollback(writeContext.PageGroups, *pageStore);
    }

    EXPECT_EQ("G=0 P=21 MV=0 V=0 D={}", formatPage2.Describe());
}
