#include "fs_directory_handle.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/buffer.h>
#include <util/stream/buffer.h>
#include <util/stream/mem.h>
#include <util/system/types.h>

#include <cstring>

namespace NCloud::NFileStore::NFuse {

namespace {

////////////////////////////////////////////////////////////////////////////////

void AssertChunksEqual(
    const TDirectoryHandleChunk& expected,
    const TDirectoryHandleChunk& actual)
{
    UNIT_ASSERT_VALUES_EQUAL(expected.Key, actual.Key);
    UNIT_ASSERT_VALUES_EQUAL(expected.Index, actual.Index);
    UNIT_ASSERT_VALUES_EQUAL(expected.UpdateVersion, actual.UpdateVersion);
    UNIT_ASSERT_VALUES_EQUAL(expected.Cookie, actual.Cookie);

    const auto* lhs = expected.DirectoryContent.Content.get();
    const auto* rhs = actual.DirectoryContent.Content.get();

    size_t lhsSize = lhs ? lhs->Size() : 0;
    size_t rhsSize = rhs ? rhs->Size() : 0;
    UNIT_ASSERT_VALUES_EQUAL(lhsSize, rhsSize);

    if (lhsSize > 0) {
        UNIT_ASSERT_VALUES_EQUAL(0, memcmp(lhs->Data(), rhs->Data(), lhsSize));
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDirectoryHandleChunkTest)
{
    Y_UNIT_TEST(ShouldSerializeAndDeserializeMinimalChunk)
    {
        TDirectoryHandleChunk original;
        original.Index = 1;
        original.UpdateVersion = 1;
        original.Cookie = "";

        TBuffer buffer;
        TBufferOutput output(buffer);
        original.Serialize(output);
        UNIT_ASSERT_VALUES_EQUAL(original.GetSerializedSize(), buffer.Size());

        TMemoryInput input(buffer.Data(), buffer.Size());
        auto deserialized = TDirectoryHandleChunk::Deserialize(input);

        UNIT_ASSERT(deserialized.has_value());
        AssertChunksEqual(original, *deserialized);
    }

    Y_UNIT_TEST(ShouldSerializeAndDeserializeChunkWithCookie)
    {
        TDirectoryHandleChunk original;
        original.Index = 1;
        original.UpdateVersion = 1;
        original.Cookie = "cookie";

        TBuffer buffer;
        TBufferOutput output(buffer);
        original.Serialize(output);
        UNIT_ASSERT_VALUES_EQUAL(original.GetSerializedSize(), buffer.Size());

        TMemoryInput input(buffer.Data(), buffer.Size());
        auto deserialized = TDirectoryHandleChunk::Deserialize(input);

        UNIT_ASSERT(deserialized.has_value());
        AssertChunksEqual(original, *deserialized);
    }

    Y_UNIT_TEST(ShouldSerializeAndDeserializeChunkWithKeyNoContent)
    {
        TDirectoryHandleChunk original;
        original.Key = 1;
        original.Index = 1;
        original.UpdateVersion = 1;
        original.Cookie = "cookie";

        TBuffer buffer;
        TBufferOutput output(buffer);
        original.Serialize(output);
        UNIT_ASSERT_VALUES_EQUAL(original.GetSerializedSize(), buffer.Size());

        TMemoryInput input(buffer.Data(), buffer.Size());
        auto deserialized = TDirectoryHandleChunk::Deserialize(input);

        UNIT_ASSERT(deserialized.has_value());
        AssertChunksEqual(original, *deserialized);
    }

    Y_UNIT_TEST(ShouldSerializeAndDeserializeChunkWithContent)
    {
        TDirectoryHandleChunk original;
        original.Key = 1;
        original.Index = 1;
        original.UpdateVersion = 1;
        original.Cookie = "cookie";

        auto content = std::make_shared<TBuffer>();
        TString testData = "Hello, this is test directory content!";
        content->Assign(testData.data(), testData.size());
        original.DirectoryContent.Content = content;

        TBuffer buffer;
        TBufferOutput output(buffer);
        original.Serialize(output);
        UNIT_ASSERT_VALUES_EQUAL(original.GetSerializedSize(), buffer.Size());

        TMemoryInput input(buffer.Data(), buffer.Size());
        auto deserialized = TDirectoryHandleChunk::Deserialize(input);

        UNIT_ASSERT(deserialized.has_value());
        AssertChunksEqual(original, *deserialized);
    }

    Y_UNIT_TEST(ShouldSerializeAndDeserializeLargeContent)
    {
        TDirectoryHandleChunk original;
        original.Key = 1;
        original.Index = 1;
        original.UpdateVersion = 1;
        original.Cookie = "cookie";

        auto content = std::make_shared<TBuffer>();
        constexpr size_t LargeSize = 64 * 1024;
        TString testData(LargeSize, 'X');

        content->Assign(testData.data(), testData.size());
        original.DirectoryContent.Content = content;

        TBuffer buffer;
        TBufferOutput output(buffer);
        original.Serialize(output);
        UNIT_ASSERT_VALUES_EQUAL(original.GetSerializedSize(), buffer.Size());

        TMemoryInput input(buffer.Data(), buffer.Size());
        auto deserialized = TDirectoryHandleChunk::Deserialize(input);

        UNIT_ASSERT(deserialized.has_value());
        AssertChunksEqual(original, *deserialized);
    }

    Y_UNIT_TEST(ShouldSerializeAndDeserializeChunkWithEmptyContent)
    {
        TDirectoryHandleChunk original;
        original.Key = 1;
        original.Index = 1;
        original.UpdateVersion = 1;
        original.Cookie = "cookie";

        auto content = std::make_shared<TBuffer>();
        original.DirectoryContent.Content = content;

        TBuffer buffer;
        TBufferOutput output(buffer);
        original.Serialize(output);
        UNIT_ASSERT_VALUES_EQUAL(original.GetSerializedSize(), buffer.Size());

        TMemoryInput input(buffer.Data(), buffer.Size());
        auto deserialized = TDirectoryHandleChunk::Deserialize(input);

        UNIT_ASSERT(deserialized.has_value());
        AssertChunksEqual(original, *deserialized);
    }

    Y_UNIT_TEST(ShouldReturnNulloptForEmptyInput)
    {
        TMemoryInput input("", 0);
        auto deserialized = TDirectoryHandleChunk::Deserialize(input);

        UNIT_ASSERT(!deserialized.has_value());
    }

    Y_UNIT_TEST(ShouldReturnNulloptForTruncatedInput)
    {
        TDirectoryHandleChunk original;
        original.Key = 1;
        original.Index = 1;
        original.UpdateVersion = 1;
        original.Cookie = "cookie";

        auto content = std::make_shared<TBuffer>();
        TString testData = "Some content here";
        content->Assign(testData.data(), testData.size());
        original.DirectoryContent.Content = content;

        TBuffer buffer;
        TBufferOutput output(buffer);
        original.Serialize(output);

        TMemoryInput input(buffer.Data(), buffer.Size() / 2);
        auto deserialized = TDirectoryHandleChunk::Deserialize(input);

        UNIT_ASSERT(!deserialized.has_value());
    }

    Y_UNIT_TEST(ShouldHandleZeroKey)
    {
        TDirectoryHandleChunk original;
        original.Key = 0;
        original.Index = 1;
        original.UpdateVersion = 1;
        original.Cookie = "cookie";

        TBuffer buffer;
        TBufferOutput output(buffer);
        original.Serialize(output);
        UNIT_ASSERT_VALUES_EQUAL(original.GetSerializedSize(), buffer.Size());

        TMemoryInput input(buffer.Data(), buffer.Size());
        auto deserialized = TDirectoryHandleChunk::Deserialize(input);

        UNIT_ASSERT(deserialized.has_value());
        AssertChunksEqual(original, *deserialized);
    }
}

}   // namespace NCloud::NFileStore::NFuse
