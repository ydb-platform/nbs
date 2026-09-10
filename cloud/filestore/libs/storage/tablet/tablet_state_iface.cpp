#include "tablet_state_iface.h"

#include <cloud/filestore/libs/storage/core/model.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

#include <util/stream/mem.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////
// Type of compression of ShardId and ShardNodeName

constexpr char ShardIdAsBinaryStream = 1;
static_assert(MinShardIdEncodingVersion <= ShardIdAsBinaryStream);
static_assert(ShardIdAsBinaryStream <= MaxShardIdEncodingVersion);

static_assert(sizeof(TGUID::dw) == 16);


inline void
DecodedShardId(const TString& mainFsId, const ui16 shardNo, TString& shardId)
{
    constexpr size_t MaxDecimalDigitsInUi16 = 5;

    shardId.ReserveAndResize(
        mainFsId.size() + ShardNumPrefix.size() + MaxDecimalDigitsInUi16);
    char* ptr = shardId.Detach();
    const char* const start = ptr;

    memcpy(ptr, mainFsId.data(), mainFsId.size());
    ptr += mainFsId.size();

    memcpy(ptr, ShardNumPrefix.data(), ShardNumPrefix.size());
    ptr += ShardNumPrefix.size();

    std::to_chars_result result =
        std::to_chars(ptr, ptr + MaxDecimalDigitsInUi16, shardNo);
    shardId.ReserveAndResize(result.ptr - start);
}

}   // namespace

bool INodeIndexTabletDatabase::TNodeRef::TryToEncodeShardId(const TString& mainFsId)
{
    if (ShardId.empty() || IsFilesystemIdEncoded(ShardId)) {
        return true;
    }

    // If ShardId is not empty it starts with mainFs.
    // If it references a shard, it is followed by '_s' + shardNo,
    // where shardNo > 0 && shardNo <= MaxShardCount

    TStringBuf shardId(ShardId);
    if (!shardId.SkipPrefix(mainFsId)) {
        return false;
    }

    ui64 shardNo = 0;
    if (shardId.size() != 0 && (!shardId.SkipPrefix(ShardNumPrefix) ||
                                !TryFromString(shardId, shardNo) ||
                                shardNo == 0 || shardNo > MaxShardCount))
    {
        return false;
    }

    TGUID guid;
    if (!GetGuid(ShardNodeName, guid)) {
        return false;
    }

    // Encode ShardId and ShardNodeName.
    ui16 shortShardNode = static_cast<ui16>(shardNo);
    ShardId.resize(sizeof(ShardIdAsBinaryStream) + sizeof(shortShardNode));
    TMemoryOutput shardIdOut(ShardId.Detach(), ShardId.size());
    shardIdOut.Write(&ShardIdAsBinaryStream, sizeof(ShardIdAsBinaryStream));
    shardIdOut.Write(&shortShardNode, sizeof(shortShardNode));

    ShardNodeName.resize(sizeof(TGUID::dw));
    TMemoryOutput shardNodeNameOut(
        ShardNodeName.Detach(),
        ShardNodeName.size());

    shardNodeNameOut.Write(guid.dw, sizeof(guid.dw));

    return true;
}

bool INodeIndexTabletDatabase::TNodeRef::TryToDecodeShardId(const TString& mainFsId)
{
    if (!IsFilesystemIdEncoded(ShardId)) {
        return true;
    }

    // One byte for the version, two bytes for the shard number.
    if (ShardId.size() != 3) {
        return false;
    }

    // The first byte in ShardId is a version.
    if (ShardId[0] != ShardIdAsBinaryStream) {
        return false;
    }

    // Get a shard number from ShardId.
    ui16 shardNo;
    memcpy(&shardNo, ShardId.data() + sizeof(char), sizeof(shardNo));

    // ShardNodeName should be GUID in binary format.
    if (ShardNodeName.size() != sizeof(TGUID::dw)) {
        return false;
    }

    // Create decoded ShardId as a string.
    if (shardNo) {
        DecodedShardId(mainFsId, shardNo, ShardId);
    } else {
        ShardId = mainFsId;
    }

    // Decode ShardNodeName
    TGUID guid;
    memcpy(guid.dw, ShardNodeName.data(), sizeof(guid.dw));
    GuidToString(guid, ShardNodeName);

    return true;
}

}   // namespace NCloud::NFileStore::NStorage
