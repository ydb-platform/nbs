#include "request_replay.h"

#include <cloud/filestore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/filestore/libs/service/request.h>
#include <cloud/filestore/tools/analytics/libs/event-log/dump.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/eventlog/eventlog.h>
#include <library/cpp/eventlog/iterator.h>
#include <library/cpp/logger/log.h>

namespace NCloud::NFileStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

IReplayRequestGenerator::IReplayRequestGenerator(
        NProto::TReplaySpec spec,
        ILoggingServicePtr logging,
        NClient::ISessionPtr session,
        TString filesystemId,
        NProto::THeaders headers)
    : Spec(std::move(spec))
    , TargetFilesystemId(std::move(filesystemId))
    , Headers(std::move(headers))
    , Session(std::move(session))
{
    Log = logging->CreateLog(Headers.GetClientId());

    NEventLog::TOptions options;
    options.FileName = Spec.GetFileName();

    // Sort eventlog items by timestamp
    options.SetForceStrongOrdering(true);
    CurrentEvent = CreateIterator(options);
}

bool IReplayRequestGenerator::HasNextRequest()
{
    if (!EventPtr) {
        Advance();
    }
    return !!EventPtr;
}

bool IReplayRequestGenerator::ShouldImmediatelyProcessQueue()
{
    return true;
}

bool IReplayRequestGenerator::ShouldFailOnError()
{
    return false;
}

void IReplayRequestGenerator::Advance()
{
    for (EventPtr = CurrentEvent->Next(); EventPtr;
         EventPtr = CurrentEvent->Next())
    {
        MessagePtr = dynamic_cast<const NProto::TProfileLogRecord*>(
            EventPtr->GetProto());

        if (!MessagePtr) {
            return;
        }

        const TString fileSystemId{MessagePtr->GetFileSystemId()};
        if (!Spec.GetFileSystemIdFilter().empty() &&
            fileSystemId != Spec.GetFileSystemIdFilter())
        {
            STORAGE_DEBUG("Skipped event with FileSystemId=" << fileSystemId);
            continue;
        }

        if (TargetFilesystemId.empty() && !fileSystemId.empty()) {
            TargetFilesystemId = fileSystemId;
            STORAGE_INFO(
                "Using FileSystemId from profile log %s",
                TargetFilesystemId.c_str());
        }

        EventMessageNumber = MessagePtr->GetRequests().size();
        return;
    }
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
                if (timediff > MaxSleepMcs) {
                    timediff = 0;
                }

                const auto current = TInstant::Now();
                auto diff = current - Started;

                if (timediff > diff.MicroSeconds()) {
                    auto sleep =
                        TDuration::MicroSeconds(timediff - diff.MicroSeconds());
                    STORAGE_DEBUG(
                        "Sleep=%lu timediff=%f diff=%lu",
                        sleep.MicroSeconds(),
                        timediff,
                        diff.MicroSeconds());

                    Sleep(sleep);
                }

                Started = current;
            }

            STORAGE_DEBUG(
                "Processing message n=%d typename=%s type=%d name=%s json=%s",
                EventMessageNumber,
                request.GetTypeName().c_str(),
                request.GetRequestType(),
                RequestName(request.GetRequestType()).c_str(),
                ToString(request.AsJSON()).c_str());
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
                            "Uninmplemented action=%u %s",
                            action,
                            RequestName(request.GetRequestType()).c_str());
                        continue;
                }
            }
        }
    }

    STORAGE_INFO(
        "Profile log finished n=%d hasPtr=%d",
        EventMessageNumber,
        !!EventPtr);

    return MakeFuture(TCompletedRequest(true));
}

}   // namespace NCloud::NFileStore::NLoadTest
