#include "blob_id.h"

#include <contrib/ydb/core/protos/base.pb.h>

#include <type_traits>
#include <utility>

namespace NCloud::NFileStore::NProtoPrivate {

////////////////////////////////////////////////////////////////////////////////

//
// TFileStoreBlobID is a local copy of NKikimrProto.TLogoBlobID. The two
// messages must stay wire-compatible: same field numbers and same scalar
// field types (checked via the getter return types).
//

static_assert(
    TFileStoreBlobID::kRawX1FieldNumber ==
    NKikimrProto::TLogoBlobID::kRawX1FieldNumber);
static_assert(
    TFileStoreBlobID::kRawX2FieldNumber ==
    NKikimrProto::TLogoBlobID::kRawX2FieldNumber);
static_assert(
    TFileStoreBlobID::kRawX3FieldNumber ==
    NKikimrProto::TLogoBlobID::kRawX3FieldNumber);

static_assert(std::is_same_v<
    decltype(std::declval<const TFileStoreBlobID&>().GetRawX1()),
    decltype(std::declval<const NKikimrProto::TLogoBlobID&>().GetRawX1())>);
static_assert(std::is_same_v<
    decltype(std::declval<const TFileStoreBlobID&>().GetRawX2()),
    decltype(std::declval<const NKikimrProto::TLogoBlobID&>().GetRawX2())>);
static_assert(std::is_same_v<
    decltype(std::declval<const TFileStoreBlobID&>().GetRawX3()),
    decltype(std::declval<const NKikimrProto::TLogoBlobID&>().GetRawX3())>);

////////////////////////////////////////////////////////////////////////////////

NKikimr::TLogoBlobID LogoBlobIDFromLogoBlobID(const TFileStoreBlobID& proto)
{
    return NKikimr::TLogoBlobID(
        proto.GetRawX1(),
        proto.GetRawX2(),
        proto.GetRawX3());
}

void LogoBlobIDFromLogoBlobID(
    const NKikimr::TLogoBlobID& id,
    TFileStoreBlobID* proto)
{
    const ui64* raw = id.GetRaw();
    proto->SetRawX1(raw[0]);
    proto->SetRawX2(raw[1]);
    proto->SetRawX3(raw[2]);
}

}   // namespace NCloud::NFileStore::NProtoPrivate

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void Convert(
    const NProtoPrivate::TFileStoreBlobID& blobId,
    NKikimrProto::TLogoBlobID& to)
{
    to.SetRawX1(blobId.GetRawX1());
    to.SetRawX2(blobId.GetRawX2());
    to.SetRawX3(blobId.GetRawX3());
}

}   // namespace NCloud::NFileStore::NStorage
