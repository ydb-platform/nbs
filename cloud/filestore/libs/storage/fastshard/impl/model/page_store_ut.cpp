#include <cloud/filestore/libs/storage/fastshard/impl/model/page_store.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/hash.h>

#include <gtest/gtest.h>

#include <functional>

using namespace NCloud;
using namespace NCloud::NFileStore::NStorage::NFastShard;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 PageSize = 16;
constexpr ui64 MaxCachedPages = 1024;
constexpr ui64 P = 1;
constexpr ui64 Q = 2;

TBuffer MakePage(char c)
{
    TBuffer page;
    page.Fill(c, PageSize);
    return page;
}

////////////////////////////////////////////////////////////////////////////////

struct TTestStorageGroup: IStorageGroup
{
    // The pages applied to the storage by the log records.
    THashMap<ui64, char> Pages;
    ui32 ReadCount = 0;
    NProto::TError ReadError;
    // Called once after the pages are read, while the page store is still
    // loading them.
    std::function<void()> OnRead;

    TResultOrError<ui64> Init() override
    {
        return 0;
    }

    void TearDown() override
    {}

    NProto::TError WriteLogRecord(
        NProto::TDeviceRequestHeaders headers,
        TVector<TPageGroup> pageGroups,
        TLsnLink link) override
    {
        Y_UNUSED(headers, pageGroups, link);
        return MakeError(E_NOT_IMPLEMENTED);
    }

    NProto::TError ReadPages(
        NProto::TDeviceRequestHeaders headers,
        const TVector<TPageGroupRef>& pageGroupRefs,
        TVector<TPageGroup>* pageGroups) override
    {
        Y_UNUSED(headers);

        ++ReadCount;
        if (HasError(ReadError)) {
            return std::exchange(ReadError, {});
        }

        for (const auto& ref: pageGroupRefs) {
            auto& pg = pageGroups->emplace_back();
            pg.FirstPageNo = ref.FirstPageNo;
            for (ui64 i = 0; i < ref.PageCount; ++i) {
                const auto* c = Pages.FindPtr(ref.FirstPageNo + i);
                pg.Content.push_back(MakePage(c ? *c : '0'));
            }
        }

        if (auto onRead = std::exchange(OnRead, {})) {
            onRead();
        }

        return {};
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TEnv
{
    std::shared_ptr<TTestStorageGroup> Storage =
        std::make_shared<TTestStorageGroup>();
    IPageStorePtr Store = CreatePageStore(
        Storage,
        {.PageSize = PageSize, .MaxCachedPages = MaxCachedPages});
    TVector<TPageGroup> LogRecord;

    void Write(ui64 lsn, ui64 pageNo, char c)
    {
        auto error = Store->WritePage(lsn, pageNo, MakePage(c), LogRecord);
        ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
    }

    [[nodiscard]] TResultOrError<char> Lookup(ui64 pageNo) const
    {
        TBuffer page;
        auto error = Store->ReadPageForLookup(pageNo, &page);
        if (HasError(error)) {
            return error;
        }
        return page.Data()[0];
    }

    [[nodiscard]] TResultOrError<char> Update(ui64 lsn, ui64 pageNo) const
    {
        TBuffer page;
        auto error = Store->ReadPageForUpdate(lsn, pageNo, &page);
        if (HasError(error)) {
            return error;
        }
        return page.Data()[0];
    }
};

#define ASSERT_PAGE(expected, actual)                                          \
    {                                                                          \
        auto result = actual;                                                  \
        ASSERT_FALSE(HasError(result)) << FormatError(result.GetError());      \
        ASSERT_EQ(expected, result.GetResult());                               \
    }                                                                          \
// ASSERT_PAGE

#define ASSERT_REJECTED(actual)                                                \
    ASSERT_EQ(E_REJECTED, (actual).GetError().GetCode())                       \
// ASSERT_REJECTED

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TEST(PageStoreTest, ShouldLoadPageOnce)
{
    TEnv env;
    env.Storage->Pages[P] = 'a';
    env.Storage->Pages[Q] = 'x';

    ASSERT_PAGE('a', env.Update(1, P));
    ASSERT_PAGE('a', env.Lookup(P));
    ASSERT_EQ(1U, env.Storage->ReadCount);

    ASSERT_PAGE('x', env.Lookup(Q));
    ASSERT_PAGE('x', env.Update(1, Q));
    ASSERT_EQ(2U, env.Storage->ReadCount);
}

TEST(PageStoreTest, ShouldReturnLastDirtyVersionForUpdate)
{
    TEnv env;
    env.Storage->Pages[P] = 'a';
    ASSERT_PAGE('a', env.Lookup(P));

    //
    // Lookup sees the committed version only, update sees the last dirty one.
    //

    env.Write(1, P, 'b');
    env.Write(2, P, 'c');
    ASSERT_PAGE('a', env.Lookup(P));
    ASSERT_PAGE('c', env.Update(2, P));

    env.Store->CommitPages(1, {P});
    ASSERT_PAGE('b', env.Lookup(P));
    ASSERT_PAGE('c', env.Update(3, P));

    env.Store->CommitPages(2, {P});
    ASSERT_PAGE('c', env.Lookup(P));
    ASSERT_PAGE('c', env.Update(3, P));
    ASSERT_EQ(1U, env.Storage->ReadCount);
}

TEST(PageStoreTest, ShouldOverwriteDirtyVersionAtSameLsn)
{
    TEnv env;

    env.Write(1, P, 'b');
    env.Write(1, P, 'c');
    env.Write(1, P + 1, 'd');
    env.Write(1, P + 3, 'e');
    ASSERT_PAGE('c', env.Update(1, P));

    ASSERT_EQ(2U, env.LogRecord.size());
    ASSERT_EQ(P, env.LogRecord[0].FirstPageNo);
    ASSERT_EQ(2U, env.LogRecord[0].Content.size());
    ASSERT_EQ('c', env.LogRecord[0].Content[0].Data()[0]);
    ASSERT_EQ(P + 3, env.LogRecord[1].FirstPageNo);

    env.Store->CommitPages(1, {P, P + 1, P + 3});
    ASSERT_PAGE('c', env.Lookup(P));
    ASSERT_EQ(0U, env.Storage->ReadCount);
}

TEST(PageStoreTest, ShouldNotLoadPageWithDirtyVersionForUpdate)
{
    TEnv env;
    env.Storage->Pages[P] = 'a';

    env.Write(1, P, 'b');
    ASSERT_PAGE('b', env.Update(1, P));
    ASSERT_EQ(0U, env.Storage->ReadCount);

    //
    // Lookup loads the committed version.
    //

    ASSERT_PAGE('a', env.Lookup(P));
    ASSERT_PAGE('b', env.Update(2, P));
    ASSERT_EQ(1U, env.Storage->ReadCount);

    env.Store->CommitPages(1, {P});
    ASSERT_PAGE('b', env.Lookup(P));
}

TEST(PageStoreTest, ShouldRollbackDirtyVersion)
{
    TEnv env;
    env.Storage->Pages[P] = 'a';
    ASSERT_PAGE('a', env.Lookup(P));

    env.Write(1, P, 'b');
    env.Write(2, P, 'c');
    env.Store->RollbackPages(2, {P});
    ASSERT_PAGE('b', env.Update(3, P));

    env.Store->RollbackPages(1, {P});
    ASSERT_PAGE('a', env.Update(3, P));
    ASSERT_PAGE('a', env.Lookup(P));

    env.Store->RollbackPages(3, {P});
    env.Store->Clear();
}

TEST(PageStoreTest, ShouldHandleReorderedCommits)
{
    TEnv env;
    env.Storage->Pages[P] = 'a';
    ASSERT_PAGE('a', env.Lookup(P));

    env.Write(1, P, 'b');
    env.Write(2, P, 'c');

    //
    // The later commit covers the earlier one.
    //

    env.Store->CommitPages(2, {P});
    ASSERT_PAGE('c', env.Lookup(P));

    env.Store->CommitPages(1, {P});
    ASSERT_PAGE('c', env.Lookup(P));
    ASSERT_PAGE('c', env.Update(3, P));
    env.Store->Clear();
}

TEST(PageStoreTest, ShouldRejectReadsWhileLoading)
{
    TEnv env;
    env.Storage->Pages[P] = 'a';

    env.Storage->OnRead = [&] {
        ASSERT_REJECTED(env.Lookup(P));
        ASSERT_REJECTED(env.Update(1, P));
    };
    ASSERT_PAGE('a', env.Lookup(P));

    //
    // A failed load is retried by the next read.
    //

    env.Storage->ReadError = MakeError(E_IO);
    ASSERT_EQ(E_IO, env.Lookup(Q).GetError().GetCode());
    ASSERT_PAGE('0', env.Update(1, Q));
    ASSERT_EQ(3U, env.Storage->ReadCount);
}

TEST(PageStoreTest, ShouldNotOverwriteVersionCommittedWhileLoading)
{
    TEnv env;
    env.Storage->Pages[P] = 'a';
    env.Write(1, P, 'b');

    //
    // The page is loaded before the log record is applied to the storage and
    // the commit arrives while the page is being loaded.
    //

    env.Storage->OnRead = [&] {
        env.Storage->Pages[P] = 'b';
        env.Store->CommitPages(1, {P});
    };
    ASSERT_PAGE('a', env.Lookup(P));

    ASSERT_PAGE('b', env.Lookup(P));
    ASSERT_PAGE('b', env.Update(2, P));
    ASSERT_EQ(1U, env.Storage->ReadCount);
}

TEST(PageStoreTest, ShouldDropDirtyVersionIncludedInLoadedOne)
{
    TEnv env;
    env.Store->InitLastLsn(10);

    env.Write(11, P, 'b');
    env.Write(12, Q, 'x');
    env.Storage->Pages[P] = 'b';
    env.Storage->Pages[Q] = 'x';

    //
    // Lsn 12 is committed before lsn 11 - the page loaded after that includes
    // lsn 11 and the commit of lsn 11 has nothing to apply.
    //

    env.Store->CommitPages(12, {Q});
    ASSERT_PAGE('b', env.Lookup(P));
    ASSERT_PAGE('b', env.Update(13, P));

    env.Store->CommitPages(11, {P});
    ASSERT_PAGE('b', env.Lookup(P));
    ASSERT_PAGE('b', env.Update(13, P));
    ASSERT_EQ(1U, env.Storage->ReadCount);

    //
    // No dirty versions are left.
    //

    env.Store->Clear();
    ASSERT_PAGE('b', env.Lookup(P));
    ASSERT_EQ(2U, env.Storage->ReadCount);
}
