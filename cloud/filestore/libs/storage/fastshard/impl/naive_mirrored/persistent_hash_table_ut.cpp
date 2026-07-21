#include <cloud/filestore/libs/storage/fastshard/impl/naive_mirrored/persistent_hash_table.h>

#include <cloud/storage/core/libs/common/error.h>

#include <gtest/gtest.h>

#include <util/generic/hash.h>
#include <util/random/fast.h>

using namespace NCloud;
using namespace NFileStore;
using namespace NStorage::NFastShard;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t PageSize = 4_KB;

////////////////////////////////////////////////////////////////////////////////

struct TSlot
{
    ui32 A = 0;
    ui64 B = 0;
    ui32 C = 0;
};

const TSlot tombstone{.B = Max<ui64>()};

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

////////////////////////////////////////////////////////////////////////////////

struct TFixture
{
    const ui64 FirstPageNo = 10;
    const ui64 PageCount = 1024;
    const ui64 SlotSize = sizeof(TSlot);
    const ui64 SlotCount = (PageSize / SlotSize) * PageCount;

    IPageStorePtr PageStore = CreateMemPageStore(PageSize);
    TPersistentHashTable<ui64, TSlot> Ht;

    TFixture()
        : Ht(
            FirstPageNo,
            PageCount,
            PageSize,
            SlotCount,
            SlotSize,
            tombstone,
            PageStore,
            [] (const TSlot& slot) {
                return slot.B;
            },
            [] (const ui64& key) {
                return IntHash(key);
            })
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TEST(PersistentHashTableTest, PutGetUpdate)
{
    TFixture fx;

    TVector<TPageGroup> pageGroups;
    auto error = fx.Ht.Put(TSlot{.A = 1, .B = 100, .C = 3}, pageGroups);
    EXPECT_EQ(S_OK, error.GetCode()) << FormatError(error);

    TSlot slot;
    ui64 slotNo = 0;
    error = fx.Ht.Get(100, &slot, &slotNo);
    EXPECT_EQ(E_REJECTED, error.GetCode()) << FormatError(error);

    Flush(pageGroups, *fx.PageStore);

    error = fx.Ht.Get(100, &slot, &slotNo);
    EXPECT_EQ(S_OK, error.GetCode()) << FormatError(error);
    EXPECT_EQ(1U, slot.A);
    EXPECT_EQ(100U, slot.B);
    EXPECT_EQ(3U, slot.C);

    fx.Ht.Update(TSlot{.A = 2, .B = 100, .C = 10}, slotNo, pageGroups);

    error = fx.Ht.Get(100, &slot, &slotNo);
    EXPECT_EQ(E_REJECTED, error.GetCode()) << FormatError(error);

    Flush(pageGroups, *fx.PageStore);

    error = fx.Ht.Get(100, &slot, &slotNo);
    EXPECT_EQ(S_OK, error.GetCode()) << FormatError(error);
    EXPECT_EQ(2U, slot.A);
    EXPECT_EQ(100U, slot.B);
    EXPECT_EQ(10U, slot.C);
}

TEST(PersistentHashTableTest, PutGetUpdateRandomized)
{
    TFixture fx;

    TVector<TPageGroup> pageGroups;

    const double putProb = 0.5;
    const double updateProb = 0.2;
    TVector<ui64> keys;
    THashMap<ui64, TSlot> refImpl;

    const ui64 seed = 111;
    TFastRng64 rng(seed);

    const ui64 maxKey = 1'000'000;

    auto makeSlot = [&] () {
        return TSlot{
            .A = static_cast<ui32>(rng.GenRand()),
            .B = rng.Uniform(0, maxKey),
            .C = static_cast<ui32>(rng.GenRand()),
        };
    };

    for (ui64 i = 0; i < fx.SlotCount; ++i) {
        //
        // Put.
        //

        if (keys.empty() || rng.GenRandReal2() < putProb) {
            auto slot = makeSlot();
            if (!refImpl.contains(slot.B)) {
                refImpl[slot.B] = slot;

                auto error = fx.Ht.Put(slot, pageGroups);
                EXPECT_EQ(S_OK, error.GetCode()) << FormatError(error);
                Flush(pageGroups, *fx.PageStore);
                keys.push_back(slot.B);
            }
        }

        //
        // Get for existing key.
        //

        TSlot slot;
        ui64 slotNo = 0;
        const ui64 key = keys[rng.Uniform(0, keys.size())];
        auto error = fx.Ht.Get(key, &slot, &slotNo);
        EXPECT_EQ(S_OK, error.GetCode()) << FormatError(error);
        auto& expectedSlot = refImpl[key];
        EXPECT_EQ(expectedSlot.A, slot.A);
        EXPECT_EQ(expectedSlot.B, slot.B);
        EXPECT_EQ(expectedSlot.C, slot.C);

        //
        // Update.
        //

        if (rng.GenRandReal2() < updateProb) {
            slot = makeSlot();
            slot.B = key;

            refImpl[key] = slot;

            error = fx.Ht.Update(slot, slotNo, pageGroups);
            EXPECT_EQ(S_OK, error.GetCode()) << FormatError(error);
            Flush(pageGroups, *fx.PageStore);
        }

        //
        // Get for missing key.
        //

        const ui64 key2 = rng.Uniform(maxKey, 1'000);
        error = fx.Ht.Get(key2, &slot, &slotNo);
        EXPECT_EQ(E_FS_NOENT, error.GetCode()) << FormatError(error);
    }
}
