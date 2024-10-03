#include "request_replay.h"

#include <cloud/filestore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/filestore/libs/service/request.h>
#include <cloud/filestore/tools/analytics/libs/event-log/dump.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/eventlog/eventlog.h>
#include <library/cpp/eventlog/iterator.h>
#include <library/cpp/logger/log.h>

namespace NCloud::NFileStore::NLoadTest {
IReplayRequestGenerator::IReplayRequestGenerator(
        NProto::TReplaySpec spec,
        ILoggingServicePtr logging,
        NClient::ISessionPtr session,
        TString filesystemId,
        NProto::THeaders headers)
    : Spec(std::move(spec))
    , FileSystemId(std::move(filesystemId))
    , Headers(std::move(headers))
    , Session(std::move(session))
{
    Log = logging->CreateLog(Headers.GetClientId());

    NEventLog::TOptions options;
    options.FileName = Spec.GetFileName();
    options.SetForceStrongOrdering(true);   // need this?
    CurrentEvent = CreateIterator(options);
}

bool IReplayRequestGenerator::HasNextRequest()
{
    if (!EventPtr) {
        Advance();
    }
    return !!EventPtr;
}

TInstant IReplayRequestGenerator::NextRequestAt()
{
    return TInstant::Max();
}

bool IReplayRequestGenerator::ShouldInstantProcessQueue()
{
    return true;
}

bool IReplayRequestGenerator::ShouldFailOnError()
{
    return false;
}

void IReplayRequestGenerator::Advance()
{
    EventPtr = CurrentEvent->Next();
    if (!EventPtr) {
        return;
    }

    MessagePtr =
        dynamic_cast<const NProto::TProfileLogRecord*>(EventPtr->GetProto());

    if (!MessagePtr) {
        return;
    }

    if (FileSystemId.empty()) {
        FileSystemId = TString{MessagePtr->GetFileSystemId()};
    }

    EventMessageNumber = MessagePtr->GetRequests().size();
}

NThreading::TFuture<TCompletedRequest>
IReplayRequestGenerator::ExecuteNextRequest()
{
    if (!HasNextRequest()) {
        return MakeFuture(TCompletedRequest(true));
    }

    for (; EventPtr; Advance()) {
        if (!MessagePtr) {
            continue;
        }

        for (; EventMessageNumber > 0;) {
            auto request = MessagePtr->GetRequests()[--EventMessageNumber];

            {
                auto timediff = (request.GetTimestampMcs() - TimestampMcs) *
                                Spec.GetTimeScale();
                TimestampMcs = request.GetTimestampMcs();
                if (timediff > 1000000) {
                    timediff = 0;
                }
                const auto current = TInstant::Now();
                auto diff = current - Started;

                if (timediff > diff.MicroSeconds()) {
                    auto slp =
                        TDuration::MicroSeconds(timediff - diff.MicroSeconds());
                    STORAGE_DEBUG(
                        "sleep=" << slp << " timediff=" << timediff
                                 << " diff=" << diff);

                    Sleep(slp);
                }
                Started = current;
            }

            STORAGE_DEBUG(
                "Processing message "
                << EventMessageNumber << " typename=" << request.GetTypeName()
                << " type=" << request.GetRequestType() << " "
                << " name=" << RequestName(request.GetRequestType())
                << " json=" << request.AsJSON());
            {
                const auto& action = request.GetRequestType();
                switch (static_cast<EFileStoreRequest>(action)) {
                    case EFileStoreRequest::ReadData:
                        return DoReadData(request);
                    case EFileStoreRequest::WriteData:
                        return DoWrite(request);
                    case EFileStoreRequest::CreateNode:
                        return DoCreateNode(request);
                    case EFileStoreRequest::RenameNode:
                        return DoRenameNode(request);
                    case EFileStoreRequest::UnlinkNode:
                        return DoUnlinkNode(request);
                    case EFileStoreRequest::CreateHandle:
                        return DoCreateHandle(request);
                    case EFileStoreRequest::DestroyHandle:
                        return DoDestroyHandle(request);
                    case EFileStoreRequest::GetNodeAttr:
                        return DoGetNodeAttr(request);
                    case EFileStoreRequest::AccessNode:
                        return DoAccessNode(request);
                    case EFileStoreRequest::ListNodes:
                        return DoListNodes(request);

                    case EFileStoreRequest::AcquireLock:
                        return DoAcquireLock(request);
                    case EFileStoreRequest::ReleaseLock:
                        return DoReleaseLock(request);

                    case EFileStoreRequest::ReadBlob:
                    case EFileStoreRequest::WriteBlob:
                    case EFileStoreRequest::GenerateBlobIds:
                    case EFileStoreRequest::PingSession:
                    case EFileStoreRequest::Ping:
                        continue;
                    default:
                        STORAGE_INFO(
                            "Uninmplemented action="
                            << action << " "
                            << RequestName(request.GetRequestType()));
                        continue;
                }
            }
        }
    }
    STORAGE_INFO(
        "Profile log finished n=" << EventMessageNumber
                                  << " ptr=" << !!EventPtr);

    return MakeFuture(TCompletedRequest(true));
}

}   // namespace NCloud::NFileStore::NLoadTest
