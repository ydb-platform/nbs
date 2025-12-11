#include "compressed_bitmap.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>
#include <util/random/random.h>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TReferenceImplementation
{
    TVector<bool> Vals;

    TReferenceImplementation(size_t size)
        : Vals(size)
    {}

    ui64 Set(ui64 b, ui64 e)
    {
        ui64 c = 0;
        while (b < e) {
            c += !Vals[b];
            Vals[b] = true;
            ++b;
        }
        return c;
    }

    ui64 Unset(ui64 b, ui64 e)
    {
        ui64 c = 0;
        while (b < e) {
            c += !!Vals[b];
            Vals[b] = false;
            ++b;
        }
        return c;
    }

    ui64 Count() const
    {
        ui64 c = 0;
        for (auto b: Vals) {
            c += !!b;
        }
        return c;
    }

    ui64 Count(ui64 b, ui64 e) const
    {
        ui64 c = 0;
        while (b < e) {
            c += !!Vals[b];
            ++b;
        }
        return c;
    }
};

ui64 AlignDownToChunk(ui64 x)
{
    return (x / TCompressedBitmap::CHUNK_SIZE) * TCompressedBitmap::CHUNK_SIZE;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCompressedBitmapTest)
{
    Y_UNIT_TEST(ExampleUsage)
    {
        TCompressedBitmap bitmap(1024 * 10);
        bitmap.Set(0, 1024);
        bitmap.Set(2048, 2048 + 512);

        UNIT_ASSERT_VALUES_EQUAL(1024 + 512, bitmap.Count());
        UNIT_ASSERT_VALUES_EQUAL(1024, bitmap.Count(0, 1024));
        UNIT_ASSERT_VALUES_EQUAL(256, bitmap.Count(0, 256));
        UNIT_ASSERT_VALUES_EQUAL(512, bitmap.Count(512, 1024));
        UNIT_ASSERT_VALUES_EQUAL(512, bitmap.Count(2048, 3072));
        UNIT_ASSERT_VALUES_EQUAL(256, bitmap.Count(2048 + 256, 3072));

        bitmap.Unset(2048, 2048 + 256);
        UNIT_ASSERT_VALUES_EQUAL(1024 + 256, bitmap.Count());
        UNIT_ASSERT_VALUES_EQUAL(256, bitmap.Count(2048, 3072));
    }

    void DoTestSetUnsetCountTest(ui64 size)
    {
        TCompressedBitmap bitmap(size);
        TReferenceImplementation ref(size);

        for (ui64 i = 0; i < 100000; ++i) {
            auto x = RandomNumber(size);
            auto xx = RandomNumber(size + 1);
            if (xx == x) {
                ++xx;
            }

            auto b = Min(x, xx);
            auto e = Max(x, xx);

            UNIT_ASSERT_VALUES_EQUAL(ref.Count(b, e), bitmap.Count(b, e));

            if (RandomNumber<bool>()) {
                UNIT_ASSERT_VALUES_EQUAL(ref.Set(b, e), bitmap.Set(b, e));
            } else {
                UNIT_ASSERT_VALUES_EQUAL(ref.Unset(b, e), bitmap.Unset(b, e));
            }

            UNIT_ASSERT_VALUES_EQUAL(ref.Count(), bitmap.Count());
        }

        for (ui64 i = 0; i < ref.Vals.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(ref.Vals[i], bitmap.Test(i));
        }
    }

    Y_UNIT_TEST(TestSetUnsetCountWithOneChunk)
    {
        DoTestSetUnsetCountTest(1024);
    }

    Y_UNIT_TEST(TestSetUnsetCountWithTenChunks)
    {
        DoTestSetUnsetCountTest(10240);
    }

    Y_UNIT_TEST(TestSetUnsetCountWithOneAndAHalfChunks)
    {
        DoTestSetUnsetCountTest(1024 + 512);
    }

    void DoTestSetUpdateCount(ui64 size)
    {
        TCompressedBitmap bitmap(size);
        TReferenceImplementation ref(size);

        for (ui64 i = 0; i < 100000; ++i) {
            auto x = RandomNumber(size);
            auto xx = RandomNumber(size + 1);
            if (xx == x) {
                ++xx;
            }

            auto b = AlignDownToChunk(Min(x, xx));
            auto e = Max(x, xx);
            auto sizePatch = 1 + RandomNumber(size);
            auto offsetPatch = RandomNumber(sizePatch);
            TCompressedBitmap update(e - b + sizePatch);
            update.Set(offsetPatch, e - b + offsetPatch);
            ref.Unset(b, Min(b + update.Capacity(), size));
            UNIT_ASSERT_VALUES_EQUAL(
                ref.Set(b + offsetPatch, Min(e + offsetPatch, size)),
                bitmap.Update(update, b));

            UNIT_ASSERT_VALUES_EQUAL(ref.Count(), bitmap.Count());

            x = RandomNumber(size);
            xx = RandomNumber(size + 1);
            if (xx == x) {
                ++xx;
            }

            b = Min(x, xx);
            e = Max(x, xx);

            bitmap.Unset(b, e);
            ref.Unset(b, e);
        }
    }

    Y_UNIT_TEST(TestSetUpdateCountWithOneChunk)
    {
        DoTestSetUpdateCount(1024);
    }

    Y_UNIT_TEST(TestSetUpdateCountWithTenChunks)
    {
        DoTestSetUpdateCount(10240);
    }

    Y_UNIT_TEST(TestMergeFromChunk)
    {
        TCompressedBitmap bitmap(1024);
        bitmap.Set(0, 100);

        TCompressedBitmap bitmapUpdate(1024);
        bitmapUpdate.Set(500, 1000);

        ui64 blocksAfterMerged = 0;

        auto serializer =
            bitmapUpdate.RangeSerializer(0, bitmapUpdate.Capacity());
        TCompressedBitmap::TSerializedChunk sc;
        while (serializer.Next(&sc)) {
            blocksAfterMerged += bitmap.Merge(sc);
        }

        UNIT_ASSERT_VALUES_EQUAL(600, blocksAfterMerged);
        UNIT_ASSERT_VALUES_EQUAL(600, bitmap.Count());
    }

    Y_UNIT_TEST(TestMergeFromChunkMany)
    {
        int numRuns = 100;

        TCompressedBitmap bitmap(1024);
        TCompressedBitmap bitmap2(1024);
        TCompressedBitmap bitmap3(1024);

        for (int i = 0; i < 1024; i += 2) {
            bitmap.Set(i, i + 1);
            bitmap2.Set(i, i + 1);
            bitmap3.Set(i + 1, i + 2);
        }

        for (int i = 0; i < numRuns; i++) {
            TCompressedBitmap& bitmapUpdate = (i % 2 == 0) ? bitmap2 : bitmap3;

            ui64 blocksAfterMerged = 0;

            auto serializer =
                bitmapUpdate.RangeSerializer(0, bitmapUpdate.Capacity());
            TCompressedBitmap::TSerializedChunk sc;
            while (serializer.Next(&sc)) {
                blocksAfterMerged += bitmap.Merge(sc);
            }

            UNIT_ASSERT_VALUES_EQUAL(blocksAfterMerged, bitmap.Count());
        }
    }

    Y_UNIT_TEST(TestSaveLoad)
    {
        const ui64 bitCount = 1ULL << 31;
        TCompressedBitmap bitmap(bitCount);
        bitmap.Set(ui64(bitCount * 0.1), ui64(bitCount * 0.3));
        bitmap.Set(ui64(bitCount * 0.45), ui64(bitCount * 0.57));
        bitmap.Set(ui64(bitCount * 0.7), ui64(bitCount * 0.71));
        bitmap.Set(ui64(bitCount * 0.9), ui64(bitCount * 0.95));

        TVector<TCompressedBitmap::TSerializedChunk> chunks;
        auto serializer = bitmap.RangeSerializer(0, bitCount);
        TCompressedBitmap::TSerializedChunk sc;
        while (serializer.Next(&sc)) {
            chunks.push_back(sc);
        }

        TCompressedBitmap bitmap2(bitCount);
        for (const auto& chunk: chunks) {
            bitmap2.Update(chunk);
        }

        UNIT_ASSERT_VALUES_EQUAL(bitmap.Count(), bitmap2.Count());
        UNIT_ASSERT_VALUES_EQUAL(bitmap.MemSize(), bitmap2.MemSize());
    }

    Y_UNIT_TEST(TestMemSize)
    {
        TCompressedBitmap bitmap(1024 * 10);
        // bitmap is empty by default
        UNIT_ASSERT_VALUES_EQUAL(0, bitmap.MemSize());

        bitmap.Set(1024, 1024 + 1024);
        UNIT_ASSERT_VALUES_EQUAL(16 * 10, bitmap.MemSize());

        bitmap.Unset(1024 + 512, 1024 + 512 + 256);
        UNIT_ASSERT_VALUES_EQUAL(16 * 10, bitmap.MemSize());

        bitmap.Unset(1024 + 256, 1024 + 256 + 128);
        UNIT_ASSERT_VALUES_EQUAL(16 * 10, bitmap.MemSize());

        bitmap.Unset(1024 + 64, 1024 + 128);
        UNIT_ASSERT_VALUES_EQUAL(16 * 10 + 128, bitmap.MemSize());

        bitmap.Set(1024, 1024 + 512);
        UNIT_ASSERT_VALUES_EQUAL(16 * 10 + 128, bitmap.MemSize());

        bitmap.Set(1024 + 512, 1024 + 1024);
        UNIT_ASSERT_VALUES_EQUAL(16 * 10, bitmap.MemSize());

        for (ui64 i = 0; i < 1024 * 10; ++i) {
            if (i % 2) {
                bitmap.Unset(i, i + 1);
            } else {
                bitmap.Set(i, i + 1);
            }
        }

        UNIT_ASSERT_VALUES_EQUAL((16 + 128) * 10, bitmap.MemSize());

        bitmap.Unset(0, 10 * 1024);
        UNIT_ASSERT_VALUES_EQUAL(16 * 10, bitmap.MemSize());
    }
}

}   // namespace NCloud
