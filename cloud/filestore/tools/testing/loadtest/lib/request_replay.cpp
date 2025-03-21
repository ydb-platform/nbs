#include "request_replay.h"

#include <cloud/filestore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
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
    options.ForceStrongOrdering = true;
    if (const auto sleep = Spec.GetMaxSleepMcs()) {
        MaxSleepMcs = sleep;
    }

    if (Spec.GetReplayTimeFrom()) {
        ReplayTimeFrom.emplace();
        if (!TInstant::TryParseIso8601(
                Spec.GetReplayTimeFrom(),
                ReplayTimeFrom.value()))
        {
            ReplayTimeFrom.reset();
        }
    }

    if (Spec.GetReplayTimeTill()) {
        ReplayTimeTill.emplace();
        if (!TInstant::TryParseIso8601(
                Spec.GetReplayTimeTill(),
                ReplayTimeTill.value()))
        {
            ReplayTimeTill.reset();
        }
    }

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
    while (EventPtr = CurrentEvent->Next()) {
        ++EventsProcessed;
        MessagePtr = dynamic_cast<const NProto::TProfileLogRecord*>(
            EventPtr->GetProto());

        if (!MessagePtr) {
            return;
        }

        const TString fileSystemId{MessagePtr->GetFileSystemId()};
        if (!Spec.GetFileSystemIdFilter().empty() &&
            fileSystemId != Spec.GetFileSystemIdFilter())
        {
            ++EventsSkipped;
            STORAGE_DEBUG(
                "Skipped events=%zu FileSystemId=%s Filter=%s",
                EventsSkipped,
                fileSystemId.c_str(),
                Spec.GetFileSystemIdFilter().c_str());
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

TFuture<TCompletedRequest> IReplayRequestGenerator::ProcessRequest(
    const NProto::TProfileLogRequestInfo& request)
{
    const auto& action = request.GetRequestType();
    switch (static_cast<EFileStoreRequest>(action)) {
        case EFileStoreRequest::ReadData:
            return DoReadData(request);
        case EFileStoreRequest::WriteData:
            return DoWriteData(request);
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
        case EFileStoreRequest::DescribeData:
        case EFileStoreRequest::AddData:
            return {};

        default:
            break;
    }

    switch (static_cast<NFuse::EFileStoreFuseRequest>(action)) {
        case NFuse::EFileStoreFuseRequest::Flush:
            return DoFlush(request);

        default:
            break;
    }

    STORAGE_DEBUG(
        "Uninmplemented action=%u %s",
        action,
        RequestName(request.GetRequestType()).c_str());

    return {};
}

NThreading::TFuture<TCompletedRequest>
IReplayRequestGenerator::ExecuteNextRequest()
{
    if (!HasNextRequest()) {
        return {};
    }
    constexpr auto OneMillion = 1000000LL;

    for (; EventPtr; Advance()) {
        if (!MessagePtr) {
            continue;
        }

        while (EventMessageNumber > 0) {
            NProto::TProfileLogRequestInfo request =
                MessagePtr->GetRequests()[--EventMessageNumber];
            {
                ++MessagesProcessed;
                i64 timediff = (static_cast<i64>(request.GetTimestampMcs()) -
                                TimestampMicroSeconds) *
                               Spec.GetTimeScale();
                TimestampMicroSeconds =
                    static_cast<i64>(request.GetTimestampMcs());
                const auto timestampSeconds =
                    TimestampMicroSeconds / OneMillion;
                if (timediff > MaxSleepMcs) {
                    STORAGE_DEBUG(
                        "Ignore too long timediff=%lu MaxSleepMcs=%lu ",
                        timediff,
                        MaxSleepMcs);

                    timediff = 0;
                }

                const auto currentInstant = TInstant::Now();

                if (NextStatusAt <= currentInstant) {
                    NextStatusAt = currentInstant + StatusEverySeconds;
                    STORAGE_INFO(
                        "Current event=%zu Skipped=%zu Msg=%zd TotalMsg=%zu "
                        "Skipped=%zu "
                        "Sleeps=%f "
                        "Time=%s",
                        EventsProcessed,
                        EventsSkipped,
                        EventMessageNumber,
                        MessagesProcessed,
                        MessagesSkipped,
                        Sleeps,
                        TInstant::MicroSeconds(TimestampMicroSeconds)
                            .ToString()
                            .c_str())
                }

                if (timestampSeconds <
                    static_cast<i64>(ReplayTimeFrom->Seconds()))
                {
                    continue;
                }

                if (timestampSeconds >
                    static_cast<i64>(ReplayTimeTill->Seconds()))
                {
                    return {};
                }
                constexpr auto RealtimeToleratePastSeconds = 100;
                constexpr auto RealtimeTolerateFutureSeconds = 1000;
                if (const i64 realTimeAlignseconds = Spec.GetRealTime()) {
                    const i64 alignMicroSeconds =
                        realTimeAlignseconds * OneMillion;
                    const i64 currentMicroSeconds =
                        currentInstant.MicroSeconds() +
                        Spec.GetRealTimeOffset() * OneMillion;
                    const i64 nowPeriodStart =
                        (currentMicroSeconds / alignMicroSeconds) *
                        alignMicroSeconds;
                    const i64 logPeriodStart =
                        (TimestampMicroSeconds / alignMicroSeconds) *
                        alignMicroSeconds;
                    const i64 nowRelativeMicroSeconds =
                        currentMicroSeconds - nowPeriodStart;
                    const i64 logRelativeMicroSeconds =
                        TimestampMicroSeconds - logPeriodStart;

                    if (nowRelativeMicroSeconds -
                            RealtimeToleratePastSeconds * OneMillion >
                        logRelativeMicroSeconds)
                    {
                        ++MessagesSkipped;
                        continue;
                    }

                    if (nowRelativeMicroSeconds < logRelativeMicroSeconds) {
                        const auto sleepMcs =
                            logRelativeMicroSeconds - nowRelativeMicroSeconds;
                        if (sleepMcs / OneMillion >
                            RealtimeTolerateFutureSeconds)
                        {
                            return {};
                        }

                        auto sleep = TDuration::MicroSeconds(sleepMcs);
                        Sleeps += sleep.SecondsFloat();
                        Sleep(sleep);
                    }
                }

                const auto diff = currentInstant - Started;
                if (timediff > static_cast<i64>(diff.MicroSeconds())) {
                    auto sleep =
                        TDuration::MicroSeconds(timediff - diff.MicroSeconds());

                    if (sleep.Seconds() > RealtimeTolerateFutureSeconds) {
                        return {};
                    }
                    STORAGE_DEBUG(
                        "Sleep=%lu timediff=%lu diff=%lu",
                        sleep.MicroSeconds(),
                        timediff,
                        diff.MicroSeconds());

                    Sleep(sleep);
                    Sleeps += sleep.SecondsFloat();
                }

                Started = currentInstant;
            }

            STORAGE_DEBUG(
                "Event=%zu Msg=%zd Mcs=%lu T=%s: Processing typename=%s "
                "type=%d name=%s "
                "data=%s",
                EventsProcessed,
                EventMessageNumber,
                request.GetTimestampMcs(),
                TInstant::MicroSeconds(request.GetTimestampMcs())
                    .ToString()
                    .c_str(),
                request.GetTypeName().c_str(),
                request.GetRequestType(),
                RequestName(request.GetRequestType()).c_str(),
                request.ShortDebugString().Quote().c_str());

            try {
                const auto future = ProcessRequest(request);
                if (future.Initialized()) {
                    return future;
                }
            } catch (const std::exception& ex) {
                STORAGE_ERROR("request exception: %s", ex.what());
            }
        }
    }

    STORAGE_INFO(
        "Profile log finished n=%zd hasPtr=%d",
        EventMessageNumber,
        !!EventPtr);

    return {};
}

}   // namespace NCloud::NFileStore::NLoadTest
