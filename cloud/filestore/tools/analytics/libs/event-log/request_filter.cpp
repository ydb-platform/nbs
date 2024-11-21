#include "request_filter.h"

#include <cloud/filestore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/filestore/libs/service/request.h>
#include <cloud/filestore/libs/storage/model/range.h>
#include <cloud/filestore/libs/storage/tablet/model/profile_log_events.h>

namespace NCloud::NFileStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TRequestFilterAccept
    : public IRequestFilter
{
public:
    NProto::TProfileLogRecord GetFilteredRecord(
        const NProto::TProfileLogRecord& record) const override
    {
        return record;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRequestFilterByFileSystemId
    : public IRequestFilter
{
private:
    const IRequestFilterPtr NextFilter;

    const TString FileSystemId;

public:
    TRequestFilterByFileSystemId(
            IRequestFilterPtr nextFilter,
            TString fileSystemId)
        : NextFilter(std::move(nextFilter))
        , FileSystemId(std::move(fileSystemId))
    {}

    NProto::TProfileLogRecord GetFilteredRecord(
        const NProto::TProfileLogRecord& record) const override
    {
        if (record.GetFileSystemId() == FileSystemId) {
            return NextFilter->GetFilteredRecord(record);
        }

        return NProto::TProfileLogRecord();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRequestFilterByNodeId
    : public IRequestFilter
{
private:
    const IRequestFilterPtr NextFilter;

    const ui64 NodeId;

public:
    TRequestFilterByNodeId(
            IRequestFilterPtr nextFilter,
            ui64 nodeId)
        : NextFilter(std::move(nextFilter))
        , NodeId(nodeId)
    {}

    NProto::TProfileLogRecord GetFilteredRecord(
        const NProto::TProfileLogRecord& record) const override
    {
        NProto::TProfileLogRecord answer;
        for (const auto& profileLogRequest: record.GetRequests()) {
            switch (profileLogRequest.GetRequestType()) {
                case static_cast<ui32>(NStorage::EFileStoreSystemRequest::ReadBlob):
                case static_cast<ui32>(NStorage::EFileStoreSystemRequest::WriteBlob):
                case static_cast<ui32>(NStorage::EFileStoreSystemRequest::DeleteGarbage):
                case static_cast<ui32>(NStorage::EFileStoreSystemRequest::CollectGarbage):
                case static_cast<ui32>(EFileStoreRequest::CreateCheckpoint):
                case static_cast<ui32>(EFileStoreRequest::DestroyCheckpoint):
                    continue;
                default:
                    break;
            }

            if (profileLogRequest.HasNodeInfo()) {
                const auto& nodeInfo = profileLogRequest.GetNodeInfo();
                if (nodeInfo.HasNodeId() && nodeInfo.GetNodeId() == NodeId) {
                    answer.MutableRequests()->Add(
                        NProto::TProfileLogRequestInfo(profileLogRequest));
                    continue;
                }
                if (nodeInfo.HasParentNodeId() &&
                    nodeInfo.GetParentNodeId() == NodeId)
                {
                    answer.MutableRequests()->Add(
                        NProto::TProfileLogRequestInfo(profileLogRequest));
                    continue;
                }
                if (nodeInfo.HasNewParentNodeId() &&
                    nodeInfo.GetNewParentNodeId() == NodeId)
                {
                    answer.MutableRequests()->Add(
                        NProto::TProfileLogRequestInfo(profileLogRequest));
                    continue;
                }
            }

            if (profileLogRequest.HasLockInfo()) {
                const auto& lockInfo = profileLogRequest.GetLockInfo();
                if (lockInfo.HasNodeId() && lockInfo.GetNodeId() == NodeId) {
                    answer.MutableRequests()->Add(
                        NProto::TProfileLogRequestInfo(profileLogRequest));
                    continue;
                }
            }

            for (const auto& range : profileLogRequest.GetRanges()) {
                if (range.HasNodeId() && range.GetNodeId() == NodeId) {
                    answer.MutableRequests()->Add(
                        NProto::TProfileLogRequestInfo(profileLogRequest));
                    break;
                }
            }
        }

        if (answer.RequestsSize() != 0) {
            answer.SetFileSystemId(record.GetFileSystemId());
            return NextFilter->GetFilteredRecord(answer);
        }

        return NProto::TProfileLogRecord();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRequestFilterByHandle
    : public IRequestFilter
{
private:
    const IRequestFilterPtr NextFilter;

    const ui64 Handle;

public:
    TRequestFilterByHandle(
            IRequestFilterPtr nextFilter,
            ui64 handle)
        : NextFilter(std::move(nextFilter))
        , Handle(handle)
    {}

    NProto::TProfileLogRecord GetFilteredRecord(
        const NProto::TProfileLogRecord& record) const override
    {
        NProto::TProfileLogRecord answer;
        for (const auto& profileLogRequest: record.GetRequests()) {
            switch (profileLogRequest.GetRequestType()) {
                case static_cast<ui32>(NStorage::EFileStoreSystemRequest::CollectGarbage):
                    continue;
                default:
                    break;
            }

            if (profileLogRequest.HasNodeInfo()) {
                const auto& nodeInfo = profileLogRequest.GetNodeInfo();
                if (nodeInfo.HasHandle() && nodeInfo.GetHandle() == Handle) {
                    answer.MutableRequests()->Add(
                        NProto::TProfileLogRequestInfo(profileLogRequest));
                    continue;
                }
            }

            if (profileLogRequest.HasLockInfo()) {
                const auto& lockInfo = profileLogRequest.GetLockInfo();
                if (lockInfo.HasHandle() && lockInfo.GetHandle() == Handle) {
                    answer.MutableRequests()->Add(
                        NProto::TProfileLogRequestInfo(profileLogRequest));
                    continue;
                }
            }

            for (const auto& range : profileLogRequest.GetRanges()) {
                if (range.HasHandle() && range.GetHandle() == Handle) {
                    answer.MutableRequests()->Add(
                        NProto::TProfileLogRequestInfo(profileLogRequest));
                    break;
                }
            }
        }

        if (answer.RequestsSize() != 0) {
            answer.SetFileSystemId(record.GetFileSystemId());
            return NextFilter->GetFilteredRecord(answer);
        }

        return NProto::TProfileLogRecord();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRequestFilterByRequestType
    : public IRequestFilter
{
private:
    const IRequestFilterPtr NextFilter;

    const TSet<ui32> RequestTypes;

public:
    TRequestFilterByRequestType(
            IRequestFilterPtr nextFilter,
            TSet<ui32> requestTypes)
        : NextFilter(std::move(nextFilter))
        , RequestTypes(std::move(requestTypes))
    {}

    NProto::TProfileLogRecord GetFilteredRecord(
        const NProto::TProfileLogRecord& record) const override
    {
        NProto::TProfileLogRecord answer;
        for (const auto& profileLogRequest: record.GetRequests()) {
            if (RequestTypes.count(profileLogRequest.GetRequestType())) {
                answer.MutableRequests()->Add(
                    NProto::TProfileLogRequestInfo(profileLogRequest));
            }
        }

        if (answer.RequestsSize() != 0) {
            answer.SetFileSystemId(record.GetFileSystemId());
            return NextFilter->GetFilteredRecord(answer);
        }

        return NProto::TProfileLogRecord();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRequestFilterByRange
    : public IRequestFilter
{
private:
    const IRequestFilterPtr NextFilter;

    const NStorage::TByteRange ByteRange;

public:
    TRequestFilterByRange(
            IRequestFilterPtr nextFilter,
            ui64 start,
            ui64 count,
            ui32 blockSize)
        : NextFilter(std::move(nextFilter))
        , ByteRange(start, count, blockSize)
    {}

    NProto::TProfileLogRecord GetFilteredRecord(
        const NProto::TProfileLogRecord& record) const override
    {
        NProto::TProfileLogRecord answer;
        for (const auto& profileLogRequest: record.GetRequests()) {
            switch (profileLogRequest.GetRequestType()) {
                case static_cast<ui32>(NStorage::EFileStoreSystemRequest::CollectGarbage):
                case static_cast<ui32>(NStorage::EFileStoreSystemRequest::DeleteGarbage):
                    continue;
                default:
                    break;
            }

            for (const auto& range: profileLogRequest.GetRanges()) {
                const NStorage::TByteRange reqRange(
                    range.GetOffset(),
                    range.GetBytes(),
                    ByteRange.BlockSize);

                if (reqRange.Overlaps(ByteRange)) {
                    answer.MutableRequests()->Add(
                        NProto::TProfileLogRequestInfo(profileLogRequest));
                    break;
                }
            }
        }

        if (answer.RequestsSize() != 0) {
            answer.SetFileSystemId(record.GetFileSystemId());
            return NextFilter->GetFilteredRecord(answer);
        }

        return NProto::TProfileLogRecord();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRequestFilterSince
    : public IRequestFilter
{
private:
    const IRequestFilterPtr NextFilter;

    const ui64 TimestampMcs;

public:
    TRequestFilterSince(
            IRequestFilterPtr nextFilter,
            ui64 timestampMcs)
        : NextFilter(std::move(nextFilter))
        , TimestampMcs(timestampMcs)
    {}

    NProto::TProfileLogRecord GetFilteredRecord(
        const NProto::TProfileLogRecord& record) const override
    {
        NProto::TProfileLogRecord answer;
        for (const auto& profileLogRequest: record.GetRequests()) {
            if (profileLogRequest.GetTimestampMcs() >= TimestampMcs) {
                answer.MutableRequests()->Add(
                    NProto::TProfileLogRequestInfo(profileLogRequest));
            }
        }

        if (answer.RequestsSize() != 0) {
            answer.SetFileSystemId(record.GetFileSystemId());
            return NextFilter->GetFilteredRecord(answer);
        }

        return NProto::TProfileLogRecord();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRequestFilterUntil
    : public IRequestFilter
{
private:
    const IRequestFilterPtr NextFilter;

    const ui64 TimestampMcs;

public:
    TRequestFilterUntil(
            IRequestFilterPtr nextFilter,
            ui64 timestampMcs)
        : NextFilter(std::move(nextFilter))
        , TimestampMcs(timestampMcs)
    {}

    NProto::TProfileLogRecord GetFilteredRecord(
        const NProto::TProfileLogRecord& record) const override
    {
        NProto::TProfileLogRecord answer;
        for (const auto& profileLogRequest: record.GetRequests()) {
            if (profileLogRequest.GetTimestampMcs() < TimestampMcs) {
                answer.MutableRequests()->Add(
                    NProto::TProfileLogRequestInfo(profileLogRequest));
            }
        }

        if (answer.RequestsSize() != 0) {
            answer.SetFileSystemId(record.GetFileSystemId());
            return NextFilter->GetFilteredRecord(answer);
        }

        return NProto::TProfileLogRecord();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IRequestFilterPtr CreateRequestFilterAccept()
{
    return std::make_shared<TRequestFilterAccept>();
}

IRequestFilterPtr CreateRequestFilterByFileSystemId(
    IRequestFilterPtr nextFilter,
    TString fileSystemId)
{
    return std::make_shared<TRequestFilterByFileSystemId>(
        std::move(nextFilter),
        std::move(fileSystemId));
}

IRequestFilterPtr CreateRequestFilterByNodeId(
    IRequestFilterPtr nextFilter,
    ui64 nodeId)
{
    return std::make_shared<TRequestFilterByNodeId>(
        std::move(nextFilter),
        nodeId);
}

IRequestFilterPtr CreateRequestFilterByHandle(
    IRequestFilterPtr nextFilter,
    ui64 handle)
{
    return std::make_shared<TRequestFilterByHandle>(
        std::move(nextFilter),
        handle);
}

IRequestFilterPtr CreateRequestFilterByRequestType(
    IRequestFilterPtr nextFilter,
    TSet<ui32> requestTypes)
{
    return std::make_shared<TRequestFilterByRequestType>(
        std::move(nextFilter),
        std::move(requestTypes));
}

IRequestFilterPtr CreateRequestFilterByRange(
    IRequestFilterPtr nextFilter,
    ui64 start,
    ui64 count,
    ui32 blockSize)
{
    return std::make_shared<TRequestFilterByRange>(
        std::move(nextFilter),
        start,
        count,
        blockSize);
}

IRequestFilterPtr CreateRequestFilterSince(
    IRequestFilterPtr nextFilter,
    ui64 timestampMcs)
{
    return std::make_shared<TRequestFilterSince>(
        std::move(nextFilter),
        timestampMcs);
}

IRequestFilterPtr CreateRequestFilterUntil(
    IRequestFilterPtr nextFilter,
    ui64 timestampMcs)
{
    return std::make_shared<TRequestFilterUntil>(
        std::move(nextFilter),
        timestampMcs);
}

}   // namespace NCloud::NFileStore
