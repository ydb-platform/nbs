#include "tablet_state_iface.h"

#include <cloud/filestore/libs/storage/core/model.h>

#include <util/generic/guid.h>

#include <util/string/builder.h>

namespace NCloud::NFileStore::NStorage {

namespace {

// Type of compression of ShardId and ShardNodeName
constexpr char ShardIdAsBinaryStream = 1;
constexpr char MinPrintableChar = ' ';
static_assert(ShardIdAsBinaryStream < MinPrintableChar);

constexpr ui64 MaxShardCount = Max<ui16>();

template <typename TNumType>
size_t NumberFromByteStream(TNumType& num, const char* byteStream)
{
    num = 0;
    for (size_t i = 0; i < sizeof(num); ++i) {
        TNumType byte = static_cast<unsigned char>(byteStream[i]);
        num |= (byte << (i * CHAR_BIT));
    }
    return sizeof(num);
}

template <typename TNumType>
void AppendStringByNumber(TNumType num, TString& str)
{
    for (size_t i = 0; i < sizeof(num); ++i) {
        str.append(static_cast<char>(num));
        num >>= CHAR_BIT;
    }
}

}   // namespace

bool IIndexTabletDatabase::TNodeRef::TryToEncodeShardId()
{
    if (ShardId.empty()) {
        return true;
    }

    // Parse ShardId and ShardNodeName that are in text format.
    const size_t pos = ShardId.rfind(ShardNumPrefix);
    ui32 shardNo = 0;
    // If ShardId does not contain "_s", it's a reference to the main
    // filesystem.
    if (pos != std::string::npos) {
        const char* pStart = ShardId.c_str() + pos + ShardNumPrefix.size();
        char* pEnd = nullptr;
        shardNo = std::strtoul(pStart, &pEnd, 10);
        // Verify that ShardId is well formed.
        if (pEnd - pStart == 0 ||
            static_cast<size_t>(pEnd - ShardId.c_str()) != ShardId.size() ||
            shardNo == 0 || shardNo > MaxShardCount)
        {
            return false;
        }
    }

    TGUID guid;
    if (!GetGuid(ShardNodeName, guid)) {
        return false;
    }

    // Encode ShardId and ShardNodeName.
    ShardId.clear();
    ShardId.append(ShardIdAsBinaryStream);
    AppendStringByNumber(static_cast<ui16>(shardNo), ShardId);

    ShardNodeName.clear();
    for (size_t i = 0; i < sizeof(guid.dw) / sizeof(guid.dw[0]); ++i) {
        AppendStringByNumber(guid.dw[i], ShardNodeName);
    }

    return true;
}

bool IIndexTabletDatabase::TNodeRef::TryToDecodeShardId(const TString& mainFs)
{
    if (!IsEncoded()) {
        return true;
    }

    // One byte for the version, two bytes for the shard number.
    // As of now, only one type of encoding exists.
    if (ShardId.size() != 3 || ShardId[0] != ShardIdAsBinaryStream) {
        return false;
    }

    // ShardNodeName should be GUID in binary format.
    if (ShardNodeName.size() != sizeof(TGUID::dw)) {
        return false;
    }

    // Decode ShardId and ShardNodeName
    ui16 shardNo = 0;
    NumberFromByteStream(shardNo, ShardId.c_str() + 1);
    ShardId = shardNo ? TStringBuilder() << mainFs << ShardNumPrefix << shardNo
                      : mainFs;

    TGUID guid;
    const char* ptr = ShardNodeName.data();
    for (size_t i = 0; i < sizeof(guid.dw) / sizeof(guid.dw[0]); ++i) {
        ptr += NumberFromByteStream(guid.dw[i], ptr);
    }
    ShardNodeName = guid.AsGuidString();

    return true;
}

}   // namespace NCloud::NFileStore::NStorage
