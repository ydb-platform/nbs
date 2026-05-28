#include "tablet_state_iface.h"

#include <cloud/filestore/libs/storage/core/model.h>

#include <util/generic/guid.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

#include <util/stream/mem.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////
// Type of compression of ShardId and ShardNodeName

constexpr char ShardIdAsBinaryStream = 1;
constexpr char MinPrintableChar = ' ';
static_assert(ShardIdAsBinaryStream < MinPrintableChar);

static_assert(sizeof(TGUID::dw) == 16);

}   // namespace

bool IIndexTabletDatabase::TNodeRef::TryToEncodeShardId(const TString& mainFs)
{
    if (ShardId.empty() || IsFilesystemIdEncoded(ShardId)) {
        return true;
    }

    // If ShardId is not empty it starts with mainFs.
    // If it references a shard, it is followed by '_s' + shardNo,
    // where shardNo > 0 && shardNo <= MaxShardCount

    TStringBuf shardId(ShardId);
    if (!shardId.SkipPrefix(mainFs)) {
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

bool IIndexTabletDatabase::TNodeRef::TryToDecodeShardId(const TString& mainFs)
{
    if (!IsFilesystemIdEncoded(ShardId)) {
        return true;
    }

    // One byte for the version, two bytes for the shard number.
    if (ShardId.size() != 3) {
        return false;
    }

    // Decode ShardId
    ui16 shardNo = 0;
    char version = 0;
    TMemoryInput shardNoIn(ShardId.data(), ShardId.size());
    shardNoIn.Read(&version, sizeof(version));
    shardNoIn.Read(&shardNo, sizeof(shardNo));

    // As of now, only one type of encoding exists.
    if (version != ShardIdAsBinaryStream) {
        return false;
    }

    ShardId = shardNo ? TStringBuilder() << mainFs << ShardNumPrefix << shardNo
                      : mainFs;

    // ShardNodeName should be GUID in binary format.
    if (ShardNodeName.size() != sizeof(TGUID::dw)) {
        return false;
    }

    // Decode ShardId and ShardNodeName
    TGUID guid;
    TMemoryInput shardNodeNameIn(ShardNodeName.data(), ShardNodeName.size());
    shardNodeNameIn.Read(guid.dw, sizeof(guid.dw));

    ShardNodeName = guid.AsGuidString();

    return true;
}

}   // namespace NCloud::NFileStore::NStorage
