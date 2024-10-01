#pragma once

#include "request.h"

#include <cloud/filestore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/filestore/libs/service/request.h>
#include <cloud/filestore/tools/analytics/libs/event-log/dump.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/eventlog/iterator.h>

namespace NCloud::NFileStore::NLoadTest {

using namespace NThreading;
using namespace NCloud::NFileStore::NClient;

class IReplayRequestGenerator: public IRequestGenerator
{
protected:
    const NProto::TReplaySpec Spec;
    TLog Log;
    TString FileSystemId;
    const NProto::THeaders Headers;
    NClient::ISessionPtr Session;

    ui64 TimestampMcs{};
    TInstant Started;

private:
    THolder<NEventLog::IIterator> EventlogIterator;
    TConstEventPtr EventPtr;
    int EventMessageNumber = 0;
    NProto::TProfileLogRecord* EventLogMessagePtr{};

public:
    IReplayRequestGenerator(
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
        EventlogIterator = CreateIterator(options);
    }

    bool InstantProcessQueue() override
    {
        return true;
    }

    bool FailOnError() override
    {
        return false;
    }

    void Advance()
    {
        EventPtr = EventlogIterator->Next();
        if (!EventPtr) {
            return;
        }

        EventLogMessagePtr = const_cast<NProto::TProfileLogRecord*>(
            dynamic_cast<const NProto::TProfileLogRecord*>(
                EventPtr->GetProto()));
        if (!EventLogMessagePtr) {
            return;
        }

        if (FileSystemId.empty()) {
            FileSystemId = TString{EventLogMessagePtr->GetFileSystemId()};
        }

        EventMessageNumber = EventLogMessagePtr->GetRequests().size();
        // DUMP(EventMessageNumber);
    }

    bool HasNextRequest() override
    {
        if (!EventPtr) {
            Advance();
        }
        // DUMP(!!EventPtr);
        return !!EventPtr;
    }

    TInstant NextRequestAt() override
    {
        return TInstant::Max();
    }

    TFuture<TCompletedRequest> ExecuteNextRequest() override
    {
        // DUMP("ex");
        if (!HasNextRequest()) {
            return MakeFuture(TCompletedRequest(true));
        }

        for (; EventPtr; Advance()) {
            if (!EventLogMessagePtr) {
                continue;
            }

            for (; EventMessageNumber > 0;) {
                auto request =
                    EventLogMessagePtr->GetRequests()[--EventMessageNumber];

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
                        auto slp = TDuration::MicroSeconds(
                            timediff - diff.MicroSeconds());
                        STORAGE_DEBUG(
                            "sleep=" << slp << " timediff=" << timediff
                                     << " diff=" << diff);

                        Sleep(slp);
                    }
                    Started = current;
                }

                STORAGE_DEBUG(
                    "Processing message "
                    << EventMessageNumber
                    << " typename=" << request.GetTypeName()
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
            "Eventlog finished n=" << EventMessageNumber
                                   << " ptr=" << !!EventPtr);

        return MakeFuture(TCompletedRequest(true));
    }

    virtual TFuture<TCompletedRequest> DoReadData(
        const NCloud::NFileStore::NProto::
            TProfileLogRequestInfo& /*unused*/) = 0;
    virtual TFuture<TCompletedRequest> DoWrite(
        const NCloud::NFileStore::NProto::
            TProfileLogRequestInfo& /*unused*/) = 0;
    virtual TFuture<TCompletedRequest> DoCreateNode(
        const NCloud::NFileStore::NProto::
            TProfileLogRequestInfo& /*unused*/) = 0;
    virtual TFuture<TCompletedRequest> DoRenameNode(
        const NCloud::NFileStore::NProto::
            TProfileLogRequestInfo& /*unused*/) = 0;
    virtual TFuture<TCompletedRequest> DoUnlinkNode(
        const NCloud::NFileStore::NProto::
            TProfileLogRequestInfo& /*unused*/) = 0;
    virtual TFuture<TCompletedRequest> DoCreateHandle(
        const NCloud::NFileStore::NProto::
            TProfileLogRequestInfo& /*unused*/) = 0;
    virtual TFuture<TCompletedRequest> DoDestroyHandle(
        const NCloud::NFileStore::NProto::
            TProfileLogRequestInfo& /*unused*/) = 0;
    virtual TFuture<TCompletedRequest> DoGetNodeAttr(
        const NCloud::NFileStore::NProto::
            TProfileLogRequestInfo& /*unused*/) = 0;
    virtual TFuture<TCompletedRequest> DoAccessNode(
        const NCloud::NFileStore::NProto::
            TProfileLogRequestInfo& /*unused*/) = 0;
    virtual TFuture<TCompletedRequest> DoListNodes(
        const NCloud::NFileStore::NProto::
            TProfileLogRequestInfo& /*unused*/) = 0;
    virtual TFuture<TCompletedRequest> DoAcquireLock(
        const NCloud::NFileStore::NProto::
            TProfileLogRequestInfo& /*unused*/) = 0;
    virtual TFuture<TCompletedRequest> DoReleaseLock(
        const NCloud::NFileStore::NProto::
            TProfileLogRequestInfo& /*unused*/) = 0;
};

}   // namespace NCloud::NFileStore::NLoadTest
