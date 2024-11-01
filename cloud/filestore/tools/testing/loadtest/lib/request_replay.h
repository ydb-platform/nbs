#pragma once

#include "request.h"

#include <cloud/filestore/libs/diagnostics/events/profile_events.ev.pb.h>

#include <library/cpp/eventlog/eventlog.h>
#include <library/cpp/eventlog/iterator.h>

namespace NCloud::NFileStore::NLoadTest {

using namespace NThreading;
using namespace NCloud::NFileStore::NClient;

////////////////////////////////////////////////////////////////////////////////

class IReplayRequestGenerator: public IRequestGenerator
{
protected:
    const ::NCloud::NFileStore::NProto::TReplaySpec Spec;
    TLog Log;
    TString FileSystemIdFilter;
    const ::NCloud::NFileStore::NProto::THeaders Headers;
    NClient::ISessionPtr Session;

    ui64 TimestampMcs{};
    TInstant Started;

    // Do not sleep too much if timestamps in log is broken
    constexpr static auto MaxSleepMcs = 1000000;

private:
    THolder<NEventLog::IIterator> CurrentEvent;
    TConstEventPtr EventPtr;
    int EventMessageNumber = 0;
    const NProto::TProfileLogRecord* MessagePtr{};
    TFuture<TCompletedRequest> ProcessRequest(
        const NProto::TProfileLogRequestInfo& request);

public:
    IReplayRequestGenerator(
        NProto::TReplaySpec spec,
        ILoggingServicePtr logging,
        NClient::ISessionPtr session,
        TString filesystemId,
        NProto::THeaders headers);

    bool ShouldImmediatelyProcessQueue() override;

    bool ShouldFailOnError() override;

    void Advance();

    bool HasNextRequest() override;

    TFuture<TCompletedRequest> ExecuteNextRequest() override;

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
    virtual TFuture<TCompletedRequest> DoFlush(
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
