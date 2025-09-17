#include "helpers.h"

#include <contrib/ydb/core/protos/filestore_config.pb.h>

namespace NCloud::NFileStore::NStorage {

using namespace NCloud::NFileStore::NProto;

namespace {

////////////////////////////////////////////////////////////////////////////////

const THashSet<TString> AllowedXAttrNamespace = {
    "security",
    "system",
    "trusted",
    "user",
};

////////////////////////////////////////////////////////////////////////////////

NProto::TNode CreateAttrs(NProto::ENodeType type, int mode, ui64 size, ui64 uid, ui64 gid)
{
    ui64 now = MicroSeconds();

    NProto::TNode node;
    node.SetType(type);
    node.SetMode(mode);
    node.SetATime(now);
    node.SetMTime(now);
    node.SetCTime(now);
    node.SetLinks(1);
    node.SetSize(size);
    node.SetUid(uid);
    node.SetGid(gid);

    return node;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

NProto::TNode CreateRegularAttrs(ui32 mode, ui32 uid, ui32 gid)
{
    return CreateAttrs(NProto::E_REGULAR_NODE, mode, 0, uid, gid);
}

NProto::TNode CreateDirectoryAttrs(ui32 mode, ui32 uid, ui32 gid)
{
    return CreateAttrs(NProto::E_DIRECTORY_NODE, mode, 0, uid, gid);
}

NProto::TNode CreateLinkAttrs(const TString& link, ui32 uid, ui32 gid)
{
    auto attrs = CreateAttrs(NProto::E_LINK_NODE, 0, link.size(), uid, gid);
    attrs.SetSymLink(link);
    return attrs;
}

NProto::TNode CreateSocketAttrs(ui32 mode, ui32 uid, ui32 gid)
{
    return CreateAttrs(NProto::E_SOCK_NODE, mode, 0, uid, gid);
}

NProto::TNode CopyAttrs(const NProto::TNode& src, ui32 mode)
{
    ui64 now = MicroSeconds();

    NProto::TNode node = src;
    if (mode & E_CM_CTIME) {
        node.SetCTime(now);
    }
    if (mode & E_CM_MTIME) {
        node.SetMTime(now);
    }
    if (mode & E_CM_ATIME) {
        node.SetATime(now);
    }
    if (mode & E_CM_REF) {
        node.SetLinks(SafeIncrement(node.GetLinks(), 1));
    }
    if (mode & E_CM_UNREF) {
        node.SetLinks(SafeDecrement(node.GetLinks(), 1));
    }

    return node;
}

void ConvertNodeFromAttrs(
    NProto::TNodeAttr& dst,
    ui64 id,
    const NProto::TNode& src)
{
    dst.SetId(id);
    dst.SetType(src.GetType());
    dst.SetMode(src.GetMode());
    dst.SetUid(src.GetUid());
    dst.SetGid(src.GetGid());
    dst.SetATime(src.GetATime());
    dst.SetMTime(src.GetMTime());
    dst.SetCTime(src.GetCTime());
    dst.SetSize(src.GetSize());
    dst.SetLinks(src.GetLinks());
}

void ConvertAttrsToNode(const NProto::TNodeAttr& src, NProto::TNode* dst)
{
    dst->SetType(src.GetType());
    dst->SetMode(src.GetMode());
    dst->SetUid(src.GetUid());
    dst->SetGid(src.GetGid());
    dst->SetATime(src.GetATime());
    dst->SetMTime(src.GetMTime());
    dst->SetCTime(src.GetCTime());
    dst->SetSize(src.GetSize());
    dst->SetLinks(src.GetLinks());
}

////////////////////////////////////////////////////////////////////////////////

NProto::TError ValidateNodeName(const TString& name)
{
    static constexpr TStringBuf Dot = ".";
    static constexpr TStringBuf DotDot = "..";

    if (name.empty() || name == Dot || name == DotDot ||
        name.find('\0') != TString::npos ||
        name.find('/') != TString::npos)
    {
        return ErrorInvalidArgument("invalid name");
    }

    if (name.size() > MaxName) {
        return ErrorNameTooLong(name);
    }

    return {};
}

NProto::TError ValidateXAttrName(const TString& name)
{
    if (name.size() > MaxXAttrName) {
        return ErrorAttributeNameTooLong(name);
    }

    TStringBuf ns;
    TStringBuf attr;
    if (!TStringBuf(name).TrySplit(".", ns, attr)) {
        return ErrorInvalidAttribute(name);
    }

    if (!AllowedXAttrNamespace.contains(ns)) {
        return ErrorInvalidAttribute(name);
    }

    return {};
}

NProto::TError ValidateXAttrValue(const TString& name, const TString& value)
{
    if (value.size() > MaxXAttrValue) {
        return ErrorAttributeValueTooBig(name);
    }

    return {};
}

NProto::TError ValidateRange(TByteRange byteRange, ui32 maxFileBlocks)
{
    if (!byteRange.Length) {
        return ErrorInvalidArgument("empty byte range");
    }

    if (byteRange.LastBlock() + 1 > maxFileBlocks) {
        return ErrorFileTooBig();
    }

    return {};
}

void Convert(
    const NKikimrFileStore::TConfig& src,
    NProto::TFileSystem& dst)
{
    for (size_t i = 0; i < src.ExplicitChannelProfilesSize(); ++i) {
        const auto& tmpSrc = src.GetExplicitChannelProfiles(i);
        auto& tmpDst = *dst.MutableExplicitChannelProfiles()->Add();

        tmpDst.SetDataKind(tmpSrc.GetDataKind());
        tmpDst.SetPoolKind(tmpSrc.GetPoolKind());
    }

    const auto mediaKind = static_cast<NCloud::NProto::EStorageMediaKind>(
        src.GetStorageMediaKind());

    dst.SetVersion(src.GetVersion());
    dst.SetFileSystemId(src.GetFileSystemId());
    dst.SetProjectId(src.GetProjectId());
    dst.SetFolderId(src.GetFolderId());
    dst.SetCloudId(src.GetCloudId());
    dst.SetBlockSize(src.GetBlockSize());
    dst.SetBlocksCount(src.GetBlocksCount());
    dst.SetNodesCount(src.GetNodesCount());
    dst.SetRangeIdHasherType(src.GetRangeIdHasherType());
    dst.SetStorageMediaKind(mediaKind);

    {
        // Performance profile.
        auto& performanceProfile = *dst.MutablePerformanceProfile();
        performanceProfile.SetThrottlingEnabled(
            src.GetPerformanceProfileThrottlingEnabled());
        performanceProfile.SetMaxReadIops(
            src.GetPerformanceProfileMaxReadIops());
        performanceProfile.SetMaxReadBandwidth(
            src.GetPerformanceProfileMaxReadBandwidth());
        performanceProfile.SetMaxWriteIops(
            src.GetPerformanceProfileMaxWriteIops());
        performanceProfile.SetMaxWriteBandwidth(
            src.GetPerformanceProfileMaxWriteBandwidth());
        performanceProfile.SetBoostTime(
            src.GetPerformanceProfileBoostTime());
        performanceProfile.SetBoostRefillTime(
            src.GetPerformanceProfileBoostRefillTime());
        performanceProfile.SetBoostPercentage(
            src.GetPerformanceProfileBoostPercentage());
        performanceProfile.SetBurstPercentage(
            src.GetPerformanceProfileBurstPercentage());
        performanceProfile.SetMaxPostponedWeight(
            src.GetPerformanceProfileMaxPostponedWeight());
        performanceProfile.SetMaxWriteCostMultiplier(
            src.GetPerformanceProfileMaxWriteCostMultiplier());
        performanceProfile.SetDefaultPostponedRequestWeight(
            src.GetPerformanceProfileDefaultPostponedRequestWeight());
        performanceProfile.SetMaxPostponedTime(
            src.GetPerformanceProfileMaxPostponedTime());
        performanceProfile.SetMaxPostponedCount(
            src.GetPerformanceProfileMaxPostponedCount());
    }
}

void Convert(
    const NProto::TFileSystem& src,
    NProtoPrivate::TFileSystemConfig& dst)
{
    for (size_t i = 0; i < src.ExplicitChannelProfilesSize(); ++i) {
        const auto& tmpSrc = src.GetExplicitChannelProfiles(i);
        auto& tmpDst = *dst.MutableExplicitChannelProfiles()->Add();

        tmpDst.SetDataKind(tmpSrc.GetDataKind());
        tmpDst.SetPoolKind(tmpSrc.GetPoolKind());
    }

    const auto mediaKind = static_cast<NCloud::NProto::EStorageMediaKind>(
        src.GetStorageMediaKind());

    dst.SetVersion(src.GetVersion());
    dst.SetFileSystemId(src.GetFileSystemId());
    dst.SetProjectId(src.GetProjectId());
    dst.SetFolderId(src.GetFolderId());
    dst.SetCloudId(src.GetCloudId());
    dst.SetBlockSize(src.GetBlockSize());
    dst.SetBlocksCount(src.GetBlocksCount());
    dst.SetNodesCount(src.GetNodesCount());
    dst.SetRangeIdHasherType(src.GetRangeIdHasherType());
    dst.SetStorageMediaKind(mediaKind);

    dst.MutablePerformanceProfile()->CopyFrom(src.GetPerformanceProfile());
}

void Convert(
    const NProto::TFileStorePerformanceProfile& src,
    TThrottlerConfig& dst)
{
    dst.ThrottlingEnabled = src.GetThrottlingEnabled();
    dst.BurstPercentage = src.GetBurstPercentage();
    dst.DefaultPostponedRequestWeight = src.GetDefaultPostponedRequestWeight();

    {
        // Default parameters.
        auto& p = dst.DefaultParameters;
        p.MaxReadIops = src.GetMaxReadIops();
        p.MaxWriteIops = src.GetMaxWriteIops();
        p.MaxReadBandwidth = src.GetMaxReadBandwidth();
        p.MaxWriteBandwidth = src.GetMaxWriteBandwidth();
    }

    {
        // Boost parameters.
        auto& p = dst.BoostParameters;
        p.BoostTime = TDuration::MilliSeconds(src.GetBoostTime());
        p.BoostRefillTime = TDuration::MilliSeconds(src.GetBoostRefillTime());
        p.BoostPercentage = src.GetBoostPercentage();
    }

    {
        // Default limits.
        auto& l = dst.DefaultThresholds;
        l.MaxPostponedWeight = src.GetMaxPostponedWeight();
        l.MaxPostponedCount = src.GetMaxPostponedCount();
        l.MaxPostponedTime = TDuration::MilliSeconds(src.GetMaxPostponedTime());
        l.MaxWriteCostMultiplier = src.GetMaxWriteCostMultiplier();
    }
}

bool IsLockingAllowed(
    const TSessionHandle* handle,
    const TLockRange& range)
{
    if (range.LockOrigin == ELockOrigin::Flock) {
        return true;
    }
    auto flags = handle->GetFlags();
    return (range.LockMode == ELockMode::Shared &&
            HasFlag(flags, NProto::TCreateHandleRequest::E_READ)) ||
           (range.LockMode == ELockMode::Exclusive &&
            HasFlag(flags, NProto::TCreateHandleRequest::E_WRITE));
}


////////////////////////////////////////////////////////////////////////////////

ELockOrigin ConvertToImpl(NProto::ELockOrigin source, TTag<ELockOrigin>)
{
    switch (source)
    {
        case NProto::E_FLOCK:
            return ELockOrigin::Flock;
        default:
            return ELockOrigin::Fcntl;
    }
}

NProto::ELockOrigin ConvertToImpl(ELockOrigin source, TTag<NProto::ELockOrigin>)
{
    switch (source)
    {
        case ELockOrigin::Flock:
            return NProto::E_FLOCK;
        case ELockOrigin::Fcntl:
            return NProto::E_FCNTL;
        default:
            Y_ABORT("Unsupported lock origin");
    }
}

ELockMode ConvertToImpl(NProto::ELockType source, TTag<ELockMode>)
{
    switch(source)
    {
        case NProto::ELockType::E_EXCLUSIVE:
            return ELockMode::Exclusive;
        case NProto::ELockType::E_SHARED:
            return ELockMode::Shared;
        case NProto::ELockType::E_UNLOCK:
            return ELockMode::Unlock;
        default:
            return ELockMode::Shared;
    }
}

NProto::ELockType ConvertToImpl(ELockMode source, TTag<NProto::ELockType>)
{
    switch (source)
    {
        case ELockMode::Shared:
            return NProto::ELockType::E_SHARED;
        case ELockMode::Exclusive:
            return NProto::ELockType::E_EXCLUSIVE;
        case ELockMode::Unlock:
            return NProto::ELockType::E_UNLOCK;
        default:
            Y_ABORT("Unsupported lock type");
    }
}

TLockRange ConvertToImpl(const NProto::TSessionLock& proto, TTag<TLockRange>)
{
    return {
        .NodeId = proto.GetNodeId(),
        .OwnerId = proto.GetOwner(),
        .Offset = proto.GetOffset(),
        .Length = proto.GetLength(),
        .Pid = proto.GetPid(),
        .LockMode = static_cast<ELockMode>(proto.GetMode()),
        .LockOrigin = ConvertTo<ELockOrigin>(proto.GetLockOrigin()),
    };
}

}   // namespace NCloud::NFileStore::NStorage
