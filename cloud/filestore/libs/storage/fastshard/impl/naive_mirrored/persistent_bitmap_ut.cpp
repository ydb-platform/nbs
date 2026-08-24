#include <cloud/filestore/libs/storage/fastshard/impl/naive_mirrored/persistent_bitmap.h>

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

////////////////////////////////////////////////////////////////////////////////

struct TFixture
{
    const ui64 FirstPageNo = 10;
    const ui64 MaxBits = 60000;

    IPageStorePtr PageStore = CreateMemPageStore(PageSize);
    std::unique_ptr<TPersistentBitmap> Bitmap;

    TFixture()
    {
        Reload();
    }

    void Reload()
    {
        Bitmap = std::make_unique<TPersistentBitmap>(
            FirstPageNo,
            MaxBits,
            PageSize,
            PageStore);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TEST(PersistentBitmapTest, SetResetAllocate)
{
    TFixture fx;

    TVector<TPageGroup> pageGroups;

    bool result = false;
    auto error = fx.Bitmap->Get(1000, &result);
    ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
    ASSERT_FALSE(result);

    error = fx.Bitmap->Set(fx.PageStore->AllocateLsn(), 1000, pageGroups);
    ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
    Flush(pageGroups, *fx.PageStore);

    result = false;
    error = fx.Bitmap->Get(1000, &result);
    ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
    ASSERT_TRUE(result);

    ui64 bit = 0;
    error = fx.Bitmap->Allocate(fx.PageStore->AllocateLsn(), &bit, pageGroups);
    ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
    Flush(pageGroups, *fx.PageStore);

    result = false;
    error = fx.Bitmap->Get(bit, &result);
    ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
    ASSERT_TRUE(result);

    ui64 bit2 = 0;
    error = fx.Bitmap->Allocate(fx.PageStore->AllocateLsn(), &bit2, pageGroups);
    ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
    Flush(pageGroups, *fx.PageStore);
    ASSERT_TRUE(bit != bit2) << bit;

    result = false;
    error = fx.Bitmap->Get(bit2, &result);
    ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
    ASSERT_TRUE(result);

    error = fx.Bitmap->Reset(fx.PageStore->AllocateLsn(), bit2, pageGroups);
    ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
    Flush(pageGroups, *fx.PageStore);

    result = false;
    error = fx.Bitmap->Get(bit2, &result);
    ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
    ASSERT_FALSE(result);

    error = fx.Bitmap->Set(fx.PageStore->AllocateLsn(), bit2, pageGroups);
    ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
    Flush(pageGroups, *fx.PageStore);

    result = false;
    error = fx.Bitmap->Get(bit2, &result);
    ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
    ASSERT_TRUE(result);

    ui64 bit3 = 0;
    error = fx.Bitmap->Allocate(fx.PageStore->AllocateLsn(), &bit3, pageGroups);
    ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
    Flush(pageGroups, *fx.PageStore);
    ASSERT_TRUE(bit != bit3) << bit;
    ASSERT_TRUE(bit2 != bit3) << bit;
}

TEST(PersistentBitmapTest, OutOfSpace)
{
    TFixture fx;

    TVector<TPageGroup> pageGroups;

    const ui64 cap = fx.MaxBits;

    THashSet<ui64> bitSet;

    for (ui64 i = 0; i < cap; ++i) {
        ui64 bit = 0;
        auto error =
            fx.Bitmap->Allocate(fx.PageStore->AllocateLsn(), &bit, pageGroups);
        ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
        Flush(pageGroups, *fx.PageStore);

        ASSERT_TRUE(bitSet.insert(bit).second);
        ASSERT_LT(bit, cap);
    }

    ui64 bit = 0;
    auto error =
        fx.Bitmap->Allocate(fx.PageStore->AllocateLsn(), &bit, pageGroups);
    ASSERT_EQ(E_FS_OUT_OF_SPACE, error.GetCode()) << FormatError(error);
    Rollback(pageGroups, *fx.PageStore);

    error = fx.Bitmap->Reset(fx.PageStore->AllocateLsn(), 1000, pageGroups);
    ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
    Flush(pageGroups, *fx.PageStore);

    bit = 0;
    error = fx.Bitmap->Allocate(fx.PageStore->AllocateLsn(), &bit, pageGroups);
    ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
    Flush(pageGroups, *fx.PageStore);
    ASSERT_EQ(1000ULL, bit);
}

TEST(PersistentBitmapTest, SetResetAllocateRandomized)
{
    TFixture fx;

    TVector<TPageGroup> pageGroups;

    const double setProb = 0.01;
    const double resetProb = 0.5;
    const double allocateProb = 0.6;
    const ui64 cap = fx.MaxBits;

    TDynBitMap refImpl;
    refImpl.Reserve(cap);

    const ui64 seed = 111;
    TFastRng64 rng(seed);

    const ui64 iters = 2 * cap;
    for (ui64 i = 0; i < iters; ++i) {
        //
        // Set.
        //

        if (rng.GenRandReal2() < setProb) {
            ui64 bit = rng.Uniform(cap);
            refImpl.Set(bit);
            auto error =
                fx.Bitmap->Set(fx.PageStore->AllocateLsn(), bit, pageGroups);
            ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
            Flush(pageGroups, *fx.PageStore);
        }

        //
        // Reset.
        //

        if (rng.GenRandReal2() < resetProb) {
            ui64 bit = rng.Uniform(cap);
            refImpl.Reset(bit);
            auto error =
                fx.Bitmap->Reset(fx.PageStore->AllocateLsn(), bit, pageGroups);
            ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
            Flush(pageGroups, *fx.PageStore);
        }

        //
        // Allocate.
        //

        if (rng.GenRandReal2() < allocateProb) {
            ui64 bit = 0;
            auto error = fx.Bitmap->Allocate(
                fx.PageStore->AllocateLsn(),
                &bit,
                pageGroups);
            ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
            Flush(pageGroups, *fx.PageStore);
            ASSERT_FALSE(refImpl.Get(bit))
                << "i=" << i << ", bit=" << bit << ", bits=" << refImpl.Count();
            refImpl.Set(bit);
        }

        //
        // Get.
        //

        ui64 bit = rng.Uniform(cap);
        bool result = false;
        auto error = fx.Bitmap->Get(bit, &result);
        ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
        ASSERT_EQ(result, refImpl.Get(bit))
            << "i=" << i << ", bit=" << bit << ", result=" << result
            << ", bits=" << refImpl.Count();

        if (i % 10'000 == 0) {
            Cdbg << "bits: " << refImpl.Count() << "/" << cap << Endl;
        }
    }

    //
    // Reload and validate.
    //

    fx.Reload();

    for (ui64 bit = 0; bit < cap; ++bit) {
        bool result = false;
        auto error = fx.Bitmap->Get(bit, &result);
        ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
        ASSERT_EQ(refImpl.Get(bit), result);
    }
}
