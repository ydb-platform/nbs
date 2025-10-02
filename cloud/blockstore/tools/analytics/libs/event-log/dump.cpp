#include "dump.h"

#include <cloud/blockstore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/blockstore/libs/service/request.h>

#include <util/generic/algorithm.h>
#include <util/string/cast.h>
#include <util/string/printf.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

using TReplicaChecksums =
    google::protobuf::RepeatedPtrField<NProto::TReplicaChecksum>;
using TRanges =
    google::protobuf::RepeatedPtrField<NProto::TProfileLogBlockRange>;
using TBlockInfos =
    google::protobuf::RepeatedPtrField<NProto::TProfileLogBlockInfo>;
using TBlockCommitIds =
    google::protobuf::RepeatedPtrField<NProto::TProfileLogBlockCommitId>;
using TBlobUpdates =
    google::protobuf::RepeatedPtrField<NProto::TProfileLogBlobUpdate>;

void OutputChecksums(
    const TReplicaChecksums& replicaChecksums,
    IOutputStream& out)
{
    if (replicaChecksums.empty()) {
        return;
    }

    out << "{";
    for (int i = 0; i < replicaChecksums.size(); ++i) {
        const auto& replicaChecksum = replicaChecksums[i];
        out << (i ? "," : "");
        out << ToString(replicaChecksum.GetReplicaId()).Quote();

        out << ":[";
        for (int j = 0; j < replicaChecksum.GetChecksums().size(); ++j) {
            out << (j ? "," : "");
            out << replicaChecksum.GetChecksums(j);
        }
        out << "]";
    }
    out << "}";
}

void OutputRanges(const TRanges& ranges, IOutputStream& out)
{
    for (int i = 0; i < ranges.size(); ++i) {
        out << (i ? "," : "");
        out << ranges[i].GetBlockIndex() << "," << ranges[i].GetBlockCount();
        OutputChecksums(ranges[i].GetReplicaChecksums(), out);
    }
}

void OutputBlockInfos(const TBlockInfos& blockInfos, IOutputStream* out)
{
    for (int i = 0; i < blockInfos.size(); ++i) {
        if (i) {
            (*out) << " ";
        }
        (*out) << blockInfos[i].GetBlockIndex()
            << ":" << blockInfos[i].GetChecksum();
    }
}

void OutputBlockCommitIds(
    const TBlockCommitIds& blockCommitIds,
    IOutputStream* out)
{
    for (int i = 0; i < blockCommitIds.size(); ++i) {
        if (i) {
            (*out) << " ";
        }
        (*out) << blockCommitIds[i].GetBlockIndex()
            << ":" << blockCommitIds[i].GetMinCommitIdOld()
            << ":" << blockCommitIds[i].GetMaxCommitIdOld()
            << ":" << blockCommitIds[i].GetMinCommitIdNew()
            << ":" << blockCommitIds[i].GetMaxCommitIdNew();
    }
}

void OutputBlobUpdates(
    const TBlobUpdates& blobUpdates,
    IOutputStream* out)
{
    for (int i = 0; i < blobUpdates.size(); ++i) {
        if (i) {
            (*out) << " ";
        }
        (*out) << blobUpdates[i].GetCommitId()
            << ":" << blobUpdates[i].GetBlockRange().GetBlockIndex()
            << "," << blobUpdates[i].GetBlockRange().GetBlockCount();
    }
}

auto GetTimestampMcs(
    const NProto::TProfileLogRecord& record,
    const TItemDescriptor& item)
{
    switch (item.Type) {
        case EItemType::Request: {
            return record.GetRequests(item.Index).GetTimestampMcs();
        }
        case EItemType::BlockInfo: {
            return record.GetBlockInfoLists(item.Index).GetTimestampMcs();
        }
        case EItemType::BlockCommitId: {
            return record.GetBlockCommitIdLists(item.Index).GetTimestampMcs();
        }
        case EItemType::BlobUpdate: {
            return record.GetBlobUpdateLists(item.Index).GetTimestampMcs();
        }
        default: {
            Y_ABORT("unknown item");
        }
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TVector<TItemDescriptor> GetItemOrder(const NProto::TProfileLogRecord& record)
{
    TVector<TItemDescriptor> order;
    order.reserve(
        record.RequestsSize() +
        record.BlockInfoListsSize() +
        record.BlockCommitIdListsSize() +
        record.BlobUpdateListsSize()
    );

    for (ui32 i = 0; i < record.RequestsSize(); ++i) {
        order.emplace_back(EItemType::Request, i);
    }
    for (ui32 i = 0; i < record.BlockInfoListsSize(); ++i) {
        order.emplace_back(EItemType::BlockInfo, i);
    }
    for (ui32 i = 0; i < record.BlockCommitIdListsSize(); ++i) {
        order.emplace_back(EItemType::BlockCommitId, i);
    }
    for (ui32 i = 0; i < record.BlobUpdateListsSize(); ++i) {
        order.emplace_back(EItemType::BlobUpdate, i);
    }

    Sort(
        order.begin(),
        order.end(),
        [&] (const auto& i, const auto& j) {
            return GetTimestampMcs(record, i) < GetTimestampMcs(record, j);
        }
    );

    return order;
}

void DumpRequest(
    const NProto::TProfileLogRecord& record,
    int i,
    IOutputStream* out)
{
    const auto& r = record.GetRequests(i);

    (*out) << TInstant::MicroSeconds(r.GetTimestampMcs())
        << "\t" << record.GetDiskId()
        << "\t" << record.GetVersion()
        << "\t" << RequestName(r.GetRequestType())
        << "\tR"
        << "\t" << r.GetDurationMcs()
        << "\t";
    if (r.GetRanges().empty()) {
        // legacy branch
        (*out) << r.GetBlockIndex()
            << "," << r.GetBlockCount();
    } else {
        OutputRanges(r.GetRanges(), *out);
    }
    (*out) << "\n";
}

void DumpBlockInfoList(
    const NProto::TProfileLogRecord& record,
    int i,
    IOutputStream* out)
{
    const auto& bl = record.GetBlockInfoLists(i);

    (*out) << TInstant::MicroSeconds(bl.GetTimestampMcs())
        << "\t" << record.GetDiskId()
        << "\t" << record.GetVersion()
        << "\t" << RequestName(bl.GetRequestType())
        << "\tB"
        << "\t" << bl.GetCommitId()
        << "\t";
    OutputBlockInfos(bl.GetBlockInfos(), out);
    (*out) << "\n";
}

void DumpBlockCommitIdList(
    const NProto::TProfileLogRecord& record,
    int i,
    IOutputStream* out)
{
    const auto& bl = record.GetBlockCommitIdLists(i);

    (*out) << TInstant::MicroSeconds(bl.GetTimestampMcs())
        << "\t" << record.GetDiskId()
        << "\t" << record.GetVersion()
        << "\t" << RequestName(bl.GetRequestType())
        << "\tC"
        << "\t" << bl.GetCommitId()
        << "\t";
    OutputBlockCommitIds(bl.GetBlockCommitIds(), out);
    (*out) << "\n";
}

void DumpBlobUpdateList(
    const NProto::TProfileLogRecord& record,
    int i,
    IOutputStream* out)
{
    const auto& bl = record.GetBlobUpdateLists(i);

    (*out) << TInstant::MicroSeconds(bl.GetTimestampMcs())
        << "\t" << record.GetDiskId()
        << "\t" << record.GetVersion()
        << "\t" << "Cleanup"
        << "\tU"
        << "\t" << bl.GetCleanupCommitId()
        << "\t";
    OutputBlobUpdates(bl.GetBlobUpdates(), out);
    (*out) << "\n";

}

TString RequestName(const ui32 requestType)
{
    TString name;
    if (requestType < static_cast<int>(EBlockStoreRequest::MAX)) {
        name = GetBlockStoreRequestName(
            static_cast<EBlockStoreRequest>(requestType)
        );
    } else if (requestType < static_cast<int>(ESysRequestType::MAX)) {
        name = GetSysRequestName(
            static_cast<ESysRequestType>(requestType)
        );
    } else {
        name = GetPrivateRequestName(
            static_cast<EPrivateRequestType>(requestType)
        );
    }

    // XXX
    if (name == "unknown") {
        name = Sprintf("unknown-%u", requestType);
    }

    return name;
}

}   // namespace NCloud::NBlockStore
