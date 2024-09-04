#include "request_printer.h"

#include <cloud/filestore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/service/request.h>
#include <cloud/filestore/libs/storage/tablet/profile_log_events.h>
#include <cloud/filestore/public/api/protos/locks.pb.h>

#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

namespace NCloud::NFileStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

auto LockType(ui32 lockType)
{
    switch (static_cast<NProto::ELockType>(lockType)) {
        case NProto::ELockType::E_EXCLUSIVE: {
            return "E_EXCLUSIVE";
        }
        case NProto::ELockType::E_SHARED: {
            return "E_SHARED";
        }
        default: {
            return "Unknown";
        }
    }
}

template <typename TValue>
TString PrintValue(TStringBuf label, const TValue& value)
{
    TStringBuilder out;

    if (!label.empty()) {
        out << label << "=";
    }
    out << value;

    return out;
}

TString PrintLockInfo(
    TStringBuf nodeIdLabel,
    TStringBuf handleLabel,
    TStringBuf ownerLabel,
    TStringBuf offsetLabel,
    TStringBuf lengthLabel,
    TStringBuf typeLabel,
    TStringBuf conflictedOwnerLabel,
    TStringBuf conflictedOffsetLabel,
    TStringBuf conflictedLengthLabel,
    const NProto::TProfileLogLockInfo& lockInfo)
{
    TStringBuilder out;

    out << "{";

    if (lockInfo.HasNodeId()) {
        out << PrintValue(nodeIdLabel, lockInfo.GetNodeId()) << ", ";
    }
    if (lockInfo.HasHandle()) {
        out << PrintValue(handleLabel, lockInfo.GetHandle()) << ", ";
    }
    if (lockInfo.HasOwner()) {
        out << PrintValue(ownerLabel, lockInfo.GetOwner()) << ", ";
    }
    if (lockInfo.HasOffset()) {
        out << PrintValue(offsetLabel, lockInfo.GetOffset()) << ", ";
    }
    if (lockInfo.HasLength()) {
        out << PrintValue(lengthLabel, lockInfo.GetLength()) << ", ";
    }
    if (lockInfo.HasType()) {
        out << PrintValue(typeLabel, LockType(lockInfo.GetType())) << ", ";
    }
    if (lockInfo.HasConflictedOwner()) {
        out << PrintValue(conflictedOwnerLabel, lockInfo.GetConflictedOwner()) << ", ";
    }
    if (lockInfo.HasConflictedOffset()) {
        out << PrintValue(conflictedOffsetLabel, lockInfo.GetConflictedOffset()) << ", ";
    }
    if (lockInfo.HasConflictedLength()) {
        out << PrintValue(conflictedLengthLabel, lockInfo.GetConflictedLength()) << ", ";
    }

    if (out.empty()) {
        out << "no_lock_info";
    } else {
        out.pop_back();
        out.pop_back();
    }

    out << "}";

    return out;
}

TString PrintNodeInfo(
    TStringBuf parentNodeIdLabel,
    TStringBuf nodeNameLabel,
    TStringBuf newParentNodeIdLabel,
    TStringBuf newNodeNameLabel,
    TStringBuf flagsLabel,
    TStringBuf modeLabel,
    TStringBuf nodeIdLabel,
    TStringBuf handleLabel,
    TStringBuf sizeLabel,
    const NProto::TProfileLogNodeInfo& nodeInfo)
{
    TStringBuilder out;

    out << "{";

    if (nodeInfo.HasParentNodeId()) {
        out << PrintValue(parentNodeIdLabel, nodeInfo.GetParentNodeId()) << ", ";
    }
    if (nodeInfo.HasNodeName()) {
        out << PrintValue(nodeNameLabel, nodeInfo.GetNodeName()) << ", ";
    }
    if (nodeInfo.HasNewParentNodeId()) {
        out << PrintValue(newParentNodeIdLabel, nodeInfo.GetNewParentNodeId()) << ", ";
    }
    if (nodeInfo.HasNewNodeName()) {
        out << PrintValue(newNodeNameLabel, nodeInfo.GetNewNodeName()) << ", ";
    }
    if (nodeInfo.HasFlags()) {
        out << PrintValue(flagsLabel, nodeInfo.GetFlags()) << ", ";
    }
    if (nodeInfo.HasMode()) {
        out << PrintValue(modeLabel, nodeInfo.GetMode()) << ", ";
    }
    if (nodeInfo.HasNodeId()) {
        out << PrintValue(nodeIdLabel, nodeInfo.GetNodeId()) << ", ";
    }
    if (nodeInfo.HasHandle()) {
        out << PrintValue(handleLabel, nodeInfo.GetHandle()) << ", ";
    }
    if (nodeInfo.HasSize()) {
        out << PrintValue(sizeLabel, nodeInfo.GetSize()) << ", ";
    }

    if (out.empty()) {
        out << "no_node_info";
    } else {
        out.pop_back();
        out.pop_back();
    }

    out << "}";

    return out;
}

TString PrintRanges(
    TStringBuf nodeIdLabel,
    TStringBuf handleLabel,
    TStringBuf offsetLabel,
    TStringBuf bytesLabel,
    const google::protobuf::RepeatedPtrField<NProto::TProfileLogBlockRange>& ranges)
{
    TStringBuilder out;

    out << "[";
    for (const auto& range : ranges) {
        out << "{";

        TStringBuilder currentRange;

        if (range.HasNodeId()) {
            currentRange << PrintValue(nodeIdLabel, range.GetNodeId()) << ", ";
        }
        if (range.HasHandle()) {
            currentRange << PrintValue(handleLabel, range.GetHandle()) << ", ";
        }
        if (range.HasOffset()) {
            currentRange << PrintValue(offsetLabel, range.GetOffset()) << ", ";
        }
        if (range.HasBytes()) {
            currentRange << PrintValue(bytesLabel, range.GetBytes()) << ", ";
        }

        if (currentRange.empty()) {
            currentRange << "no_range_info";
        } else {
            currentRange.pop_back();
            currentRange.pop_back();
        }

        out << currentRange << "}, ";
    }

    if (out.empty()) {
        out << "no_ranges";
    } else {
        out.pop_back();
        out.pop_back();
    }

    out << "]";

    return out;
}

TString PrintBlobsInfo(
    const google::protobuf::RepeatedPtrField<NProto::TProfileLogBlobInfo>& blobs)
{
    TStringBuilder out;

    out << "[";
    for (const auto& blob : blobs) {

        TPartialBlobId id(blob.GetCommitId(), blob.GetUnique());

        out << id << "->";
        out << PrintRanges(
                "node_id",
                "handle",
                "offset",
                "bytes",
                blob.GetRanges())
            << '\t';
    }

    if (out.back() == '\t') {
        out.pop_back();
    }

    out << "]";
    return out;
}

TString PrintCompactionRanges(
    const google::protobuf::RepeatedPtrField<NProto::TProfileLogCompactionRangeInfo>& ranges)
{
    TStringBuilder out;

    out << "[";
    for (const auto& range : ranges) {
        out << "{";

        TStringBuilder currentRange;

        if (range.HasCommitId()) {
            currentRange << PrintValue("commitId", range.GetCommitId()) << ", ";
        }
        if (range.HasRangeId()) {
            currentRange << PrintValue("rangeId", range.GetRangeId()) << ", ";
        }
        if (range.HasBlobsCount()) {
            currentRange << PrintValue("blobsCount", range.GetBlobsCount()) << ", ";
        }
        if (range.HasDeletionsCount()) {
            currentRange << PrintValue("deletionsCount", range.GetDeletionsCount()) << ", ";
        }

        if (currentRange.empty()) {
            currentRange << "no_compaction_range_info";
        } else {
            currentRange.pop_back();
            currentRange.pop_back();
        }

        out << currentRange << "}, ";
    }

    if (out.empty()) {
        out << "no_ranges";
    } else {
        out.pop_back();
        out.pop_back();
    }

    out << "]";

    return out;
}

////////////////////////////////////////////////////////////////////////////////

class TDefaultRequestPrinter
    : public IRequestPrinter
{
public:
    TString DumpInfo(const NProto::TProfileLogRequestInfo& request) const override
    {
        TStringBuilder out;

        if (request.HasNodeInfo()) {
            out << PrintNodeInfo(
                "parent_node_id",
                "node_name",
                "new_parent_node_id",
                "new_node_name",
                "flags",
                "mode",
                "node_id",
                "handle",
                "size",
                request.GetNodeInfo()) << "\t";
        }

        if (request.HasLockInfo()) {
            out << PrintLockInfo(
                "node_id",
                "handle",
                "owner",
                "offset",
                "length",
                "type",
                "conflicted_owner",
                "conflicted_offset",
                "conflicted_length",
                request.GetLockInfo()) << "\t";
        }

        if (!request.GetRanges().empty()) {
            out << PrintRanges(
                "node_id",
                "handle",
                "offset",
                "bytes",
                request.GetRanges()) << "\t";
        }

        if (!request.GetCompactionRanges().empty()) {
            out << PrintCompactionRanges(request.GetCompactionRanges()) << "\t";
        }

        if (!request.GetBlobsInfo().empty()) {
            out << PrintBlobsInfo(request.GetBlobsInfo()) << "\t";
        }

        if (out.empty()) {
            out << "{no_info}";
        } else {
            out.pop_back();
        }

        return out;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TAccessNodeRequestPrinter
    : public IRequestPrinter
{
public:
    TString DumpInfo(const NProto::TProfileLogRequestInfo& request) const override
    {
        if (request.HasNodeInfo()) {
            return PrintNodeInfo(
                "",
                "",
                "",
                "",
                "mask",
                "",
                "node_id",
                "",
                "",
                request.GetNodeInfo());
        }

        return "{no_info}";
    }
};

////////////////////////////////////////////////////////////////////////////////

class TGetSetNodeXAttrRequestPrinter
    : public IRequestPrinter
{
public:
    TString DumpInfo(const NProto::TProfileLogRequestInfo& request) const override
    {
        if (request.HasNodeInfo()) {
            return PrintNodeInfo(
                "",
                "attr_name",
                "",
                "attr_value",
                "flags",
                "",
                "node_id",
                "",
                "version",
                request.GetNodeInfo());
        }

        return "{no_info}";
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRemoveNodeXAttrRequestPrinter
    : public IRequestPrinter
{
public:
    TString DumpInfo(const NProto::TProfileLogRequestInfo& request) const override
    {
        if (request.HasNodeInfo()) {
            return PrintNodeInfo(
                "node_id",
                "attr_name",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                request.GetNodeInfo());
        }

        return "{no_info}";
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCreateDestroyCheckpointRequestPrinter
    : public IRequestPrinter
{
public:
    TString DumpInfo(const NProto::TProfileLogRequestInfo& request) const override
    {
        if (request.HasNodeInfo()) {
            return PrintNodeInfo(
                "",
                "checkpoint_id",
                "",
                "",
                "",
                "",
                "node_id",
                "",
                "",
                request.GetNodeInfo());
        }

        return "{no_info}";
    }
};

////////////////////////////////////////////////////////////////////////////////

class TFlushFsyncRequestPrinter
    : public IRequestPrinter
{
public:
    TString DumpInfo(const NProto::TProfileLogRequestInfo& request) const override
    {
        if (request.HasNodeInfo()) {
            return PrintNodeInfo(
                "",
                "",
                "",
                "",
                "",
                "data_only",
                "node_id",
                "handle",
                "",
                request.GetNodeInfo());
        }

        return "{no_info}";
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCollectGarbageRequestPrinter
    : public IRequestPrinter
{
public:
    TString DumpInfo(const NProto::TProfileLogRequestInfo& request) const override
    {
        if (!request.GetRanges().empty()) {
            return PrintRanges(
                "current_collect_commid_id",
                "last_collect_commit_id",
                "new_blobs",
                "garbage_blobs",
                request.GetRanges());
        }

        return "{no_info}";
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDeleteGarbageRequestPrinter
    : public IRequestPrinter
{
public:
    TString DumpInfo(const NProto::TProfileLogRequestInfo& request) const override
    {
        if (!request.GetRanges().empty()) {
            return PrintRanges(
                "collect_commit_id",
                "",
                "new_blobs",
                "garbage_blobs",
                request.GetRanges());
        }

        return "{no_info}";
    }
};

////////////////////////////////////////////////////////////////////////////////

class TReadWriteBlobRequestPrinter
    : public IRequestPrinter
{
public:
    TString DumpInfo(const NProto::TProfileLogRequestInfo& request) const override
    {
        if (!request.GetRanges().empty()) {
            return PrintRanges(
                "commit_id",
                "",
                "offset",
                "bytes",
                request.GetRanges());
        }

        return "{no_info}";
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IRequestPrinterPtr CreateRequestPrinter(ui32 requestType)
{
    if (requestType < static_cast<ui32>(EFileStoreRequest::MAX)) {
        switch (static_cast<EFileStoreRequest>(requestType)) {
            case EFileStoreRequest::AccessNode:
                return std::make_shared<TAccessNodeRequestPrinter>();
            case EFileStoreRequest::SetNodeXAttr:
            case EFileStoreRequest::GetNodeXAttr:
                return std::make_shared<TGetSetNodeXAttrRequestPrinter>();
            case EFileStoreRequest::RemoveNodeXAttr:
                return std::make_shared<TRemoveNodeXAttrRequestPrinter>();
            case EFileStoreRequest::CreateCheckpoint:
            case EFileStoreRequest::DestroyCheckpoint:
                return std::make_shared<TCreateDestroyCheckpointRequestPrinter>();
            default:
                break;
        }
    } else if (
        requestType > static_cast<ui32>(NFuse::EFileStoreFuseRequest::MIN) &&
        requestType < static_cast<ui32>(NFuse::EFileStoreFuseRequest::MAX))
    {
        switch (static_cast<NFuse::EFileStoreFuseRequest>(requestType)) {
            case NFuse::EFileStoreFuseRequest::Flush:
            case NFuse::EFileStoreFuseRequest::Fsync:
            case NFuse::EFileStoreFuseRequest::FsyncDir:
                return std::make_shared<TFlushFsyncRequestPrinter>();
            default:
                break;
        }
    } else if (
        requestType > static_cast<ui32>(NStorage::EFileStoreSystemRequest::MIN) &&
        requestType < static_cast<ui32>(NStorage::EFileStoreSystemRequest::MAX))
    {
        switch (static_cast<NStorage::EFileStoreSystemRequest>(requestType)) {
            case NStorage::EFileStoreSystemRequest::CollectGarbage:
                return std::make_shared<TCollectGarbageRequestPrinter>();
            case NStorage::EFileStoreSystemRequest::DeleteGarbage:
                return std::make_shared<TDeleteGarbageRequestPrinter>();
            case NStorage::EFileStoreSystemRequest::ReadBlob:
            case NStorage::EFileStoreSystemRequest::WriteBlob:
                return std::make_shared<TReadWriteBlobRequestPrinter>();
            default:
                break;
        }
    }

    return std::make_shared<TDefaultRequestPrinter>();
}

}   // namespace NCloud::NFileStore
