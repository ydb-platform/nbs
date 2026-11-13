#include "compressed_bitmap_serialization.h"

#include <cloud/storage/core/protos/compressed_bitmap.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <limits>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString MakeSerializedChunkData()
{
    TCompressedBitmap bitmap(TCompressedBitmap::CHUNK_SIZE);
    bitmap.Set(0, 1);

    auto serializer = bitmap.RangeSerializer(0, bitmap.Capacity());
    TCompressedBitmap::TSerializedChunk chunk;
    UNIT_ASSERT(serializer.Next(&chunk));

    return TString(chunk.Data.data(), chunk.Data.size());
}

std::unique_ptr<TCompressedBitmap> LoadCompressedBitmapForTest(
    const NProto::TCompressedBitmapData& proto,
    const ui64 minBitCount,
    const ui64 maxBitCount)
{
    auto result = LoadCompressedBitmap(proto, minBitCount, maxBitCount);
    UNIT_ASSERT_C(!HasError(result.GetError()), FormatError(result.GetError()));
    return result.ExtractResult();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCompressedBitmapProtoTest)
{
    NProto::TCompressedBitmapData MakeCompressedBitmapData()
    {
        TCompressedBitmap bitmap(4 * TCompressedBitmap::CHUNK_SIZE);
        bitmap.Set(0, 1);
        bitmap.Set(
            2 * TCompressedBitmap::CHUNK_SIZE,
            2 * TCompressedBitmap::CHUNK_SIZE + 1);

        NProto::TCompressedBitmapData proto;
        SaveCompressedBitmap(bitmap, bitmap.Capacity(), proto);
        return proto;
    }

    Y_UNIT_TEST(ShouldValidateAndLoadCompressedBitmapData)
    {
        const auto proto = MakeCompressedBitmapData();

        auto loaded = LoadCompressedBitmapForTest(
            proto,
            proto.GetBitCount(),
            proto.GetBitCount());
        UNIT_ASSERT_VALUES_EQUAL(2, loaded->Count());
        UNIT_ASSERT(loaded->Test(0));
        UNIT_ASSERT(loaded->Test(2 * TCompressedBitmap::CHUNK_SIZE));
    }

    Y_UNIT_TEST(ShouldRoundTripEmptyCompressedBitmapData)
    {
        TCompressedBitmap bitmap(1);

        NProto::TCompressedBitmapData proto;
        SaveCompressedBitmap(bitmap, 0, proto);

        UNIT_ASSERT_VALUES_EQUAL(0, proto.GetBitCount());
        UNIT_ASSERT_VALUES_EQUAL(0, proto.ChunksSize());

        auto loaded = LoadCompressedBitmapForTest(proto, 0, 0);
        UNIT_ASSERT_VALUES_EQUAL(0, loaded->Count());
    }

    Y_UNIT_TEST(ShouldSkipZeroChunksOnSave)
    {
        TCompressedBitmap bitmap(2 * TCompressedBitmap::CHUNK_SIZE);
        bitmap.Set(
            TCompressedBitmap::CHUNK_SIZE,
            TCompressedBitmap::CHUNK_SIZE + 1);

        NProto::TCompressedBitmapData proto;
        SaveCompressedBitmap(bitmap, bitmap.Capacity(), proto);

        UNIT_ASSERT_VALUES_EQUAL(bitmap.Capacity(), proto.GetBitCount());
        UNIT_ASSERT_VALUES_EQUAL(1, proto.ChunksSize());
        UNIT_ASSERT_VALUES_EQUAL(1, proto.GetChunks(0).GetChunkIdx());
    }

    Y_UNIT_TEST(ShouldRespectMinBitCountOnLoad)
    {
        NProto::TCompressedBitmapData proto;
        proto.SetBitCount(1);
        auto* chunk = proto.AddChunks();
        chunk->SetChunkIdx(0);
        chunk->SetData(MakeSerializedChunkData());

        auto loaded = LoadCompressedBitmapForTest(
            proto,
            2 * TCompressedBitmap::CHUNK_SIZE,
            2 * TCompressedBitmap::CHUNK_SIZE);

        UNIT_ASSERT_VALUES_EQUAL(
            2 * TCompressedBitmap::CHUNK_SIZE,
            loaded->Capacity());
        UNIT_ASSERT_VALUES_EQUAL(1, loaded->Count());
        UNIT_ASSERT(loaded->Test(0));
    }

    Y_UNIT_TEST(ShouldValidateCompressedBitmapData)
    {
        const auto bitmap = MakeCompressedBitmapData();

        UNIT_ASSERT(!HasError(
            ValidateCompressedBitmapData(bitmap, bitmap.GetBitCount())));
    }

    Y_UNIT_TEST(ShouldRejectInvalidCompressedBitmapData)
    {
        const auto validData = MakeSerializedChunkData();

        NProto::TCompressedBitmapData bitmap;
        bitmap.SetBitCount(TCompressedBitmap::CHUNK_SIZE + 1);

        auto error =
            ValidateCompressedBitmapData(bitmap, TCompressedBitmap::CHUNK_SIZE);
        UNIT_ASSERT(HasError(error));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());

        bitmap.SetBitCount(1);
        auto* chunk = bitmap.AddChunks();
        chunk->SetChunkIdx(0);
        chunk->SetData(validData);
        chunk = bitmap.AddChunks();
        chunk->SetChunkIdx(0);
        chunk->SetData(validData);

        error =
            ValidateCompressedBitmapData(bitmap, TCompressedBitmap::CHUNK_SIZE);
        UNIT_ASSERT(HasError(error));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());

        bitmap.ClearChunks();
        chunk = bitmap.AddChunks();
        chunk->SetChunkIdx(1);
        chunk->SetData(validData);

        error =
            ValidateCompressedBitmapData(bitmap, TCompressedBitmap::CHUNK_SIZE);
        UNIT_ASSERT(HasError(error));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());

        chunk->SetChunkIdx(0);
        chunk->ClearData();

        error =
            ValidateCompressedBitmapData(bitmap, TCompressedBitmap::CHUNK_SIZE);
        UNIT_ASSERT(HasError(error));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
    }

    Y_UNIT_TEST(ShouldRejectInvalidCompressedBitmapDataOnLoad)
    {
        const auto validData = MakeSerializedChunkData();

        NProto::TCompressedBitmapData proto;
        proto.SetBitCount(1);
        auto* chunk = proto.AddChunks();
        chunk->SetChunkIdx(1);
        chunk->SetData(validData);

        auto result = LoadCompressedBitmap(
            proto,
            2 * TCompressedBitmap::CHUNK_SIZE,
            2 * TCompressedBitmap::CHUNK_SIZE);
        UNIT_ASSERT(HasError(result.GetError()));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, result.GetError().GetCode());
        UNIT_ASSERT_C(
            result.GetError().GetMessage().Contains(
                "Compressed bitmap chunk index is out of range"),
            result.GetError().GetMessage());
    }

    Y_UNIT_TEST(ShouldRejectHugeBitCountOnLoad)
    {
        NProto::TCompressedBitmapData proto;
        proto.SetBitCount(std::numeric_limits<ui64>::max());

        auto result =
            LoadCompressedBitmap(proto, 0, TCompressedBitmap::CHUNK_SIZE);
        UNIT_ASSERT(HasError(result.GetError()));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, result.GetError().GetCode());
        UNIT_ASSERT_C(
            result.GetError().GetMessage().Contains(
                "Compressed bitmap bit count exceeds limit"),
            result.GetError().GetMessage());
    }

    Y_UNIT_TEST(ShouldRejectMinBitCountAboveLimitOnLoad)
    {
        NProto::TCompressedBitmapData proto;

        auto result = LoadCompressedBitmap(proto, 2, 1);
        UNIT_ASSERT(HasError(result.GetError()));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, result.GetError().GetCode());
        UNIT_ASSERT_C(
            result.GetError().GetMessage().Contains(
                "Compressed bitmap min bit count exceeds limit"),
            result.GetError().GetMessage());
    }

    Y_UNIT_TEST(ShouldSaveOnlyChunksWithinBitmapCapacity)
    {
        TCompressedBitmap bitmap(TCompressedBitmap::CHUNK_SIZE);
        bitmap.Set(0, 1);

        NProto::TCompressedBitmapData proto;
        SaveCompressedBitmap(bitmap, 2 * TCompressedBitmap::CHUNK_SIZE, proto);

        UNIT_ASSERT_VALUES_EQUAL(
            2 * TCompressedBitmap::CHUNK_SIZE,
            proto.GetBitCount());
        UNIT_ASSERT_VALUES_EQUAL(1, proto.ChunksSize());
        UNIT_ASSERT_VALUES_EQUAL(0, proto.GetChunks(0).GetChunkIdx());
    }
}

}   // namespace NCloud
