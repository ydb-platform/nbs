#include "tablet_state_iface.h"

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

void IIndexTabletDatabase::TNodeRef::EncodeShardId()
{
    if (ShardId.empty()) {
        return;
    }

    const size_t pos = ShardId.rfind("_s") + 2;
    // If ShardId does not contain '_s', it's a reference to the main filesystem
    ui32 shardNo = 0;
    if (pos < ShardId.size()) {
        char* p_end{};
        shardNo = std::strtoul(ShardId.c_str() + pos, &p_end, 10);
        Y_ABORT_UNLESS(shardNo <= MaxShardCount);
    }

    ShardId.clear();
    ShardId.append(ShardIdAsBinaryStream);
    AppendStringByNumber(static_cast<ui16>(shardNo), ShardId);

    TGUID guid;
    Y_ABORT_UNLESS(GetGuid(ShardNodeName, guid));
    ShardNodeName.clear();
    for (size_t i = 0; i < sizeof(guid.dw) / sizeof(guid.dw[0]); ++i) {
        AppendStringByNumber(guid.dw[i], ShardNodeName);
    }
}

void IIndexTabletDatabase::TNodeRef::DecodeShardId(const TString& mainFs)
{
    if (!IsEncoded()) {
        return;
    }

    // One byte for the version, two bytes for the shard number.
    // As of now, only one type of encoding exists.
    Y_ABORT_UNLESS(ShardId.size() == 3 && ShardId[0] == ShardIdAsBinaryStream);
    ui16 shardNo = 0;
    NumberFromByteStream(shardNo, ShardId.c_str() + 1);
    ShardId = shardNo ? TStringBuilder() << mainFs << "_s" << shardNo : mainFs;

    Y_ABORT_UNLESS(ShardNodeName.size() == sizeof(TGUID::dw));
    TGUID guid;
    const char* ptr = ShardNodeName.data();
    for (size_t i = 0; i < sizeof(guid.dw) / sizeof(guid.dw[0]); ++i) {
        ptr += NumberFromByteStream(guid.dw[i], ptr);
    }
    ShardNodeName = guid.AsGuidString();
}

}   // namespace NCloud::NFileStore::NStorage
