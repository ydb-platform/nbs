#include <cloud/filestore/libs/storage/fastshard/impl/model/persistent_hash_table.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/hash.h>
#include <util/random/fast.h>

#include <gtest/gtest.h>

using namespace NCloud;
using namespace NFileStore;
using namespace NFileStore::NStorage::NFastShard;
using namespace NCloud::NFastShard;

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

void Flush(TWriteContext& writeContext, IPageStore& pageStore)
{
    pageStore.CommitPages(
        writeContext.Lsn,
        CollectPages(writeContext.PageGroups));
}

////////////////////////////////////////////////////////////////////////////////

struct TFixture
{
    const ui64 FirstPageNo = 10;
    const ui64 PageCount = 1024;
    const ui64 SlotSize = sizeof(TSlot);

    IPageStorePtr PageStore = CreateMemPageStore(PageSize);
    TPersistentHashTable<ui64, TSlot> Ht;

    TFixture()
        : Ht(
              FirstPageNo,
              PageCount,
              PageSize,
              SlotSize,
              tombstone,
              PageStore,
              [](const TSlot& slot) { return slot.B; },
              [](const ui64& key) { return IntHash(key); })
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TEST(PersistentHashTableTest, PutGetUpdate)
{
    TFixture fx;

    {
        TWriteContext writeContext;
        writeContext.Lsn = fx.PageStore->AllocateLsn();

        auto error = fx.Ht.Put(
            writeContext.Lsn,
            TSlot{.A = 1, .B = 100, .C = 3},
            writeContext.PageGroups);
        EXPECT_EQ(S_OK, error.GetCode()) << FormatError(error);

        TSlot slot;
        ui64 slotNo = 0;
        error = fx.Ht.GetForLookup(100, &slot, &slotNo);
        EXPECT_EQ(E_FS_NOENT, error.GetCode()) << FormatError(error);

        error = fx.Ht.GetForUpdate(writeContext.Lsn, 100, &slot, &slotNo);
        EXPECT_EQ(S_OK, error.GetCode()) << FormatError(error);
        EXPECT_EQ(1U, slot.A);
        EXPECT_EQ(100U, slot.B);
        EXPECT_EQ(3U, slot.C);

        Flush(writeContext, *fx.PageStore);
    }

    {
        TWriteContext writeContext;
        writeContext.Lsn = fx.PageStore->AllocateLsn();

        TSlot slot;
        ui64 slotNo = 0;

        auto error = fx.Ht.GetForUpdate(writeContext.Lsn, 100, &slot, &slotNo);
        EXPECT_EQ(S_OK, error.GetCode()) << FormatError(error);
        EXPECT_EQ(1U, slot.A);
        EXPECT_EQ(100U, slot.B);
        EXPECT_EQ(3U, slot.C);

        error = fx.Ht.Update(
            writeContext.Lsn,
            TSlot{.A = 2, .B = 100, .C = 10},
            slotNo,
            writeContext.PageGroups);
        EXPECT_EQ(S_OK, error.GetCode()) << FormatError(error);

        error = fx.Ht.GetForLookup(100, &slot, &slotNo);
        EXPECT_EQ(S_OK, error.GetCode()) << FormatError(error);
        EXPECT_EQ(1U, slot.A);
        EXPECT_EQ(100U, slot.B);
        EXPECT_EQ(3U, slot.C);

        error = fx.Ht.GetForUpdate(writeContext.Lsn, 100, &slot, &slotNo);
        EXPECT_EQ(S_OK, error.GetCode()) << FormatError(error);
        EXPECT_EQ(2U, slot.A);
        EXPECT_EQ(100U, slot.B);
        EXPECT_EQ(10U, slot.C);

        Flush(writeContext, *fx.PageStore);
    }

    {
        TSlot slot;
        ui64 slotNo = 0;
        auto error = fx.Ht.GetForLookup(100, &slot, &slotNo);
        EXPECT_EQ(S_OK, error.GetCode()) << FormatError(error);
        EXPECT_EQ(2U, slot.A);
        EXPECT_EQ(100U, slot.B);
        EXPECT_EQ(10U, slot.C);
    }
}

TEST(PersistentHashTableTest, Delete)
{
    TFixture fx;

    {
        TWriteContext writeContext;
        writeContext.Lsn = fx.PageStore->AllocateLsn();
        auto error = fx.Ht.Put(
            writeContext.Lsn,
            TSlot{.A = 1, .B = 100, .C = 3},
            writeContext.PageGroups);
        EXPECT_EQ(S_OK, error.GetCode()) << FormatError(error);
        Flush(writeContext, *fx.PageStore);
    }

    {
        TWriteContext writeContext;
        writeContext.Lsn = fx.PageStore->AllocateLsn();
        auto error = fx.Ht.Put(
            writeContext.Lsn,
            TSlot{.A = 2, .B = 101, .C = 4},
            writeContext.PageGroups);
        EXPECT_EQ(S_OK, error.GetCode()) << FormatError(error);
        Flush(writeContext, *fx.PageStore);
    }

    {
        TWriteContext writeContext;
        writeContext.Lsn = fx.PageStore->AllocateLsn();
        auto error = fx.Ht.Put(
            writeContext.Lsn,
            TSlot{.A = 3, .B = 102, .C = 5},
            writeContext.PageGroups);
        EXPECT_EQ(S_OK, error.GetCode()) << FormatError(error);
        Flush(writeContext, *fx.PageStore);
    }

    {
        TWriteContext writeContext;
        writeContext.Lsn = fx.PageStore->AllocateLsn();
        TSlot slot;
        auto error = fx.Ht.Delete(
            writeContext.Lsn,
            101,
            &slot,
            writeContext.PageGroups);
        EXPECT_EQ(2U, slot.A);
        EXPECT_EQ(101U, slot.B);
        EXPECT_EQ(4U, slot.C);
        Flush(writeContext, *fx.PageStore);
    }

    {
        TSlot slot;
        ui64 slotNo = 0;
        auto error = fx.Ht.GetForLookup(100, &slot, &slotNo);
        EXPECT_EQ(S_OK, error.GetCode()) << FormatError(error);
        EXPECT_EQ(1U, slot.A);
        EXPECT_EQ(100U, slot.B);
        EXPECT_EQ(3U, slot.C);
        error = fx.Ht.GetForLookup(101, &slot, &slotNo);
        EXPECT_EQ(E_FS_NOENT, error.GetCode()) << FormatError(error);
        error = fx.Ht.GetForLookup(102, &slot, &slotNo);
        EXPECT_EQ(S_OK, error.GetCode()) << FormatError(error);
        EXPECT_EQ(3U, slot.A);
        EXPECT_EQ(102U, slot.B);
        EXPECT_EQ(5U, slot.C);
    }

    {
        TSlot slot;
        TWriteContext writeContext;
        writeContext.Lsn = fx.PageStore->AllocateLsn();
        auto error = fx.Ht.Delete(
            writeContext.Lsn,
            100,
            &slot,
            writeContext.PageGroups);
        EXPECT_EQ(1U, slot.A);
        EXPECT_EQ(100U, slot.B);
        EXPECT_EQ(3U, slot.C);
        Flush(writeContext, *fx.PageStore);
    }

    {
        TSlot slot;
        ui64 slotNo = 0;
        auto error = fx.Ht.GetForLookup(100, &slot, &slotNo);
        EXPECT_EQ(E_FS_NOENT, error.GetCode()) << FormatError(error);
        error = fx.Ht.GetForLookup(101, &slot, &slotNo);
        EXPECT_EQ(E_FS_NOENT, error.GetCode()) << FormatError(error);
        error = fx.Ht.GetForLookup(102, &slot, &slotNo);
        EXPECT_EQ(S_OK, error.GetCode()) << FormatError(error);
        EXPECT_EQ(3U, slot.A);
        EXPECT_EQ(102U, slot.B);
        EXPECT_EQ(5U, slot.C);
    }

    {
        TSlot slot;
        TWriteContext writeContext;
        writeContext.Lsn = fx.PageStore->AllocateLsn();
        auto error = fx.Ht.Delete(
            writeContext.Lsn,
            102,
            &slot,
            writeContext.PageGroups);
        EXPECT_EQ(3U, slot.A);
        EXPECT_EQ(102U, slot.B);
        EXPECT_EQ(5U, slot.C);
        Flush(writeContext, *fx.PageStore);
    }

    {
        TSlot slot;
        ui64 slotNo = 0;
        auto error = fx.Ht.GetForLookup(100, &slot, &slotNo);
        EXPECT_EQ(E_FS_NOENT, error.GetCode()) << FormatError(error);
        error = fx.Ht.GetForLookup(101, &slot, &slotNo);
        EXPECT_EQ(E_FS_NOENT, error.GetCode()) << FormatError(error);
        error = fx.Ht.GetForLookup(102, &slot, &slotNo);
        EXPECT_EQ(E_FS_NOENT, error.GetCode()) << FormatError(error);
    }
}

TEST(PersistentHashTableTest, PutGetUpdateRandomized)
{
    TFixture fx;

    const double putProb = 0.5;
    const double updateProb = 0.2;
    const double deleteProb = 0.3;
    TVector<ui64> keys;
    THashMap<ui64, std::pair<TSlot, ui64>> refImpl;

    const ui64 seed = 111;
    TFastRng64 rng(seed);

    const ui64 maxKey = 1'000'000;

    auto makeSlot = [&]()
    {
        return TSlot{
            .A = static_cast<ui32>(rng.GenRand()),
            .B = rng.Uniform(0, maxKey),
            .C = static_cast<ui32>(rng.GenRand()),
        };
    };

    for (ui64 i = 0; i < fx.Ht.GetSlotCount(); ++i) {
        //
        // Put.
        //

        if (keys.empty() || rng.GenRandReal2() < putProb) {
            auto slot = makeSlot();
            if (!refImpl.contains(slot.B)) {
                refImpl[slot.B] = {slot, keys.size()};

                TWriteContext writeContext;
                writeContext.Lsn = fx.PageStore->AllocateLsn();
                auto error = fx.Ht.Put(
                    writeContext.Lsn,
                    slot,
                    writeContext.PageGroups);
                Cdbg << "PUT " << slot.B << Endl;
                ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
                Flush(writeContext, *fx.PageStore);
                keys.push_back(slot.B);
            }
        }

        //
        // Get for existing key.
        //

        TSlot slot;
        ui64 slotNo = 0;
        const ui64 key = keys[rng.Uniform(0, keys.size())];
        auto error = fx.Ht.GetForLookup(key, &slot, &slotNo);
        Cdbg << "GET " << key << Endl;
        ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
        auto& expectedSlot = refImpl[key].first;
        ASSERT_EQ(expectedSlot.A, slot.A);
        ASSERT_EQ(expectedSlot.B, slot.B);
        ASSERT_EQ(expectedSlot.C, slot.C);

        //
        // Update.
        //

        if (rng.GenRandReal2() < updateProb) {
            slot = makeSlot();
            slot.B = key;

            refImpl[key].first = slot;

            TWriteContext writeContext;
            writeContext.Lsn = fx.PageStore->AllocateLsn();
            error = fx.Ht.Update(
                writeContext.Lsn,
                slot,
                slotNo,
                writeContext.PageGroups);
            Cdbg << "UPDATE " << key << Endl;
            ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
            Flush(writeContext, *fx.PageStore);
        }

        //
        // Delete.
        //

        if (rng.GenRandReal2() < deleteProb) {
            auto it = refImpl.find(key);
            Y_ABORT_UNLESS(it != refImpl.end());
            const ui64 keysArrayIndex = it->second.second;
            if (keysArrayIndex != keys.size() - 1) {
                refImpl[keys.back()].second = keysArrayIndex;
                keys[keysArrayIndex] = keys.back();
            }
            keys.pop_back();
            refImpl.erase(it);

            TWriteContext writeContext;
            writeContext.Lsn = fx.PageStore->AllocateLsn();
            error = fx.Ht.Delete(writeContext.Lsn, key, &slot, writeContext.PageGroups);
            Cdbg << "DELETE " << key << Endl;
            ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
            Flush(writeContext, *fx.PageStore);

            error = fx.Ht.GetForLookup(key, &slot, &slotNo);
            if (error.GetCode() != E_FS_NOENT) {
                Cerr << slot.B << Endl;
            }
            ASSERT_EQ(E_FS_NOENT, error.GetCode()) << FormatError(error);
        }

        //
        // Get for missing key.
        //

        const ui64 key2 = rng.Uniform(maxKey, 1'000);
        error = fx.Ht.GetForLookup(key2, &slot, &slotNo);
        ASSERT_EQ(E_FS_NOENT, error.GetCode()) << FormatError(error);

        //
        // Output stats from time to time.
        //

        if (i % 10'000 == 0) {
            TPersistentHashTableStats stats;
            error = fx.Ht.CollectStats(&stats);
            ASSERT_EQ(S_OK, error.GetCode()) << FormatError(error);
            Cerr << "values=" << stats.ValueCount << "/" << stats.SlotCount
                 << " tombstones=" << stats.TombstoneCount << "/"
                 << stats.SlotCount
                 << " misplaced=" << stats.MisplacedValueCount << "/"
                 << stats.ValueCount << Endl;
        }
    }
}
