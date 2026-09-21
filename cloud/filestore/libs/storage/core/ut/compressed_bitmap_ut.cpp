#include <cloud/filestore/libs/storage/core/compressed_bitmap.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCompressedBitmapProtoTest)
{
    NProtoPrivate::TCompressedBitmapData MakeCompressedBitmapData()
    {
        TCompressedBitmap bitmap(4 * TCompressedBitmap::CHUNK_SIZE);
        bitmap.Set(0, 1);
        bitmap.Set(
            2 * TCompressedBitmap::CHUNK_SIZE,
            2 * TCompressedBitmap::CHUNK_SIZE + 1);

        NProtoPrivate::TCompressedBitmapData proto;
        SaveCompressedBitmap(bitmap, bitmap.Capacity(), proto);
        return proto;
    }

    Y_UNIT_TEST(ShouldValidateAndLoadCompressedBitmapData)
    {
        const auto proto = MakeCompressedBitmapData();

        auto loaded = LoadCompressedBitmap(proto, proto.GetBitCount());
        UNIT_ASSERT_VALUES_EQUAL(2, loaded.Count());
        UNIT_ASSERT(loaded.Test(0));
        UNIT_ASSERT(loaded.Test(2 * TCompressedBitmap::CHUNK_SIZE));
    }

    Y_UNIT_TEST(ShouldRoundTripEmptyCompressedBitmapData)
    {
        TCompressedBitmap bitmap(1);

        NProtoPrivate::TCompressedBitmapData proto;
        SaveCompressedBitmap(bitmap, 0, proto);

        UNIT_ASSERT_VALUES_EQUAL(0, proto.GetBitCount());
        UNIT_ASSERT_VALUES_EQUAL(0, proto.ChunksSize());

        auto loaded = LoadCompressedBitmap(proto, 0);
        UNIT_ASSERT_VALUES_EQUAL(0, loaded.Count());
    }

    Y_UNIT_TEST(ShouldSkipZeroChunksOnSave)
    {
        TCompressedBitmap bitmap(2 * TCompressedBitmap::CHUNK_SIZE);
        bitmap.Set(
            TCompressedBitmap::CHUNK_SIZE,
            TCompressedBitmap::CHUNK_SIZE + 1);

        NProtoPrivate::TCompressedBitmapData proto;
        SaveCompressedBitmap(bitmap, bitmap.Capacity(), proto);

        UNIT_ASSERT_VALUES_EQUAL(bitmap.Capacity(), proto.GetBitCount());
        UNIT_ASSERT_VALUES_EQUAL(1, proto.ChunksSize());
        UNIT_ASSERT_VALUES_EQUAL(1, proto.GetChunks(0).GetChunkIdx());
    }

    Y_UNIT_TEST(ShouldRespectMinBitCountOnLoad)
    {
        NProtoPrivate::TCompressedBitmapData proto;
        proto.SetBitCount(1);
        auto* chunk = proto.AddChunks();
        chunk->SetChunkIdx(0);
        chunk->SetData(MakeSerializedChunkData());

        auto loaded = LoadCompressedBitmap(
            proto,
            2 * TCompressedBitmap::CHUNK_SIZE);

        UNIT_ASSERT_VALUES_EQUAL(
            2 * TCompressedBitmap::CHUNK_SIZE,
            loaded.Capacity());
        UNIT_ASSERT_VALUES_EQUAL(1, loaded.Count());
        UNIT_ASSERT(loaded.Test(0));
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

        NProtoPrivate::TCompressedBitmapData bitmap;
        bitmap.SetBitCount(TCompressedBitmap::CHUNK_SIZE + 1);

        auto error = ValidateCompressedBitmapData(
            bitmap,
            TCompressedBitmap::CHUNK_SIZE);
        UNIT_ASSERT(HasError(error));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());

        bitmap.SetBitCount(1);
        auto* chunk = bitmap.AddChunks();
        chunk->SetChunkIdx(0);
        chunk->SetData(validData);
        chunk = bitmap.AddChunks();
        chunk->SetChunkIdx(0);
        chunk->SetData(validData);

        error = ValidateCompressedBitmapData(
            bitmap,
            TCompressedBitmap::CHUNK_SIZE);
        UNIT_ASSERT(HasError(error));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());

        bitmap.ClearChunks();
        chunk = bitmap.AddChunks();
        chunk->SetChunkIdx(1);
        chunk->SetData(validData);

        error = ValidateCompressedBitmapData(
            bitmap,
            TCompressedBitmap::CHUNK_SIZE);
        UNIT_ASSERT(HasError(error));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());

        chunk->SetChunkIdx(0);
        chunk->ClearData();

        error = ValidateCompressedBitmapData(
            bitmap,
            TCompressedBitmap::CHUNK_SIZE);
        UNIT_ASSERT(HasError(error));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
    }

    Y_UNIT_TEST(ShouldSkipOutOfRangeChunksOnLoad)
    {
        const auto validData = MakeSerializedChunkData();

        NProtoPrivate::TCompressedBitmapData proto;
        proto.SetBitCount(1);
        auto* chunk = proto.AddChunks();
        chunk->SetChunkIdx(1);
        chunk->SetData(validData);

        auto loaded = LoadCompressedBitmap(
            proto,
            2 * TCompressedBitmap::CHUNK_SIZE);
        UNIT_ASSERT_VALUES_EQUAL(0, loaded.Count());
    }

    Y_UNIT_TEST(ShouldSaveOnlyChunksWithinBitmapCapacity)
    {
        TCompressedBitmap bitmap(TCompressedBitmap::CHUNK_SIZE);
        bitmap.Set(0, 1);

        NProtoPrivate::TCompressedBitmapData proto;
        SaveCompressedBitmap(bitmap, 2 * TCompressedBitmap::CHUNK_SIZE, proto);

        UNIT_ASSERT_VALUES_EQUAL(
            2 * TCompressedBitmap::CHUNK_SIZE,
            proto.GetBitCount());
        UNIT_ASSERT_VALUES_EQUAL(1, proto.ChunksSize());
        UNIT_ASSERT_VALUES_EQUAL(0, proto.GetChunks(0).GetChunkIdx());
    }
}

}   // namespace NCloud::NFileStore::NStorage
