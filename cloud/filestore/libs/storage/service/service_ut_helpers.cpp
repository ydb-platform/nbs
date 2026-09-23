#include "service_ut_helpers.h"

#include <cloud/filestore/libs/storage/tablet/events/tablet_private.h>
#include <cloud/filestore/libs/storage/testlib/service_client.h>

#include <library/cpp/testing/unittest/registar.h>

#include <utility>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

void TTestProfileLog::Start()
{}

void TTestProfileLog::Stop()
{}

void TTestProfileLog::Write(TRecord record)
{
    UNIT_ASSERT(record.Request.HasRequestType());
    Requests[record.Request.GetRequestType()].push_back(std::move(record));
}

void TTestProfileLog::RegisterCounters(NMonitoring::TDynamicCounters& root)
{
    Y_UNUSED(root);
}

TString GenerateValidateData(ui32 size, ui32 seed)
{
    TString data(size, 0);
    for (ui32 i = 0; i < size; ++i) {
        data[i] = 'A' + ((i + seed) % ('Z' - 'A' + 1));
    }
    return data;
}

void WaitForTabletStart(TServiceClient& service)
{
    TDispatchOptions options;
    options.FinalEvents = {
        TDispatchOptions::TFinalEventCondition(
            TEvIndexTabletPrivate::EvLoadCompactionMapChunkRequest)};
    service.AccessRuntime().DispatchEvents(options, TDuration::Seconds(5));
}

NProtoPrivate::TGetStorageStatsResponse GetStorageStats(
    TServiceClient& service,
    const TString& fsId,
    const ui64 cacheTTL,
    const NProtoPrivate::EStatsRequestMode mode)
{
    NProtoPrivate::TGetStorageStatsRequest request;
    request.SetFileSystemId(fsId);
    request.SetCacheTTL(cacheTTL);
    request.SetMode(mode);
    TString buf;
    google::protobuf::util::MessageToJsonString(request, &buf);
    const auto actionResponse = service.ExecuteAction("GetStorageStats", buf);
    NProtoPrivate::TGetStorageStatsResponse response;
    auto status = google::protobuf::util::JsonStringToMessage(
        actionResponse->Record.GetOutput(),
        &response);

    return response;
}

void CreateOrResizeFilesystem(
    TServiceClient& service,
    const TString& fsId,
    ui64 fsBlocksCount,
    bool resize,
    TMap<TString, TActorId>& fsToActor)
{
    fsToActor.clear();

    TActorId mainActorId{};

    bool configureShardsRequestObserved = false;
    auto prevFilter = service.AccessRuntime().SetEventFilter(
        [&](auto& runtime, TAutoPtr<IEventHandle>& event)
        {
            Y_UNUSED(runtime);
            switch (event->GetTypeRewrite()) {
                case TEvIndexTablet::EvConfigureAsShardRequest: {
                    using R = TEvIndexTablet::TEvConfigureAsShardRequest;
                    const auto* msg = event->Get<R>();
                    fsToActor[msg->Record.GetFileSystemId()] = event->Recipient;
                    break;
                }

                case TEvIndexTablet::EvConfigureShardsRequest: {
                    configureShardsRequestObserved = true;
                    break;
                }

                case TEvIndexTabletPrivate::EvLoadCompactionMapChunkRequest: {
                    // The first tablet to start after ConfigureShards
                    // request is sent is the main tablet (after suiciding)
                    if (configureShardsRequestObserved) {
                        mainActorId = event->Recipient;
                        fsToActor[fsId] = event->Recipient;
                    }
                    break;
                }
            }

            return false;
        });

    if (resize) {
        service.ResizeFileStore(fsId, fsBlocksCount);
    } else {
        service.CreateFileStore(fsId, fsBlocksCount);
    }

    service.AccessRuntime().DispatchEvents(
        {.CustomFinalCondition = [&]() -> bool
         {
             return static_cast<bool>(mainActorId);
         }});

    service.AccessRuntime().SetEventFilter(prevFilter);
}

void UpdateCounters(
    TTestEnv& env,
    TServiceClient& service,
    ui32 nodeIdx,
    TActorId fsActorId)
{
    using TRequest = TEvIndexTabletPrivate::TEvUpdateCounters;
    env.GetRuntime().Send(
        new IEventHandle(
            fsActorId, // recipient
            TActorId(), // sender
            new TRequest(),
            0, // flags
            0),
        nodeIdx);

    TDispatchOptions options;
    options.FinalEvents = {
        TDispatchOptions::TFinalEventCondition(
            TEvIndexTabletPrivate::EvAggregateStatsCompleted)};

    service.AccessRuntime().DispatchEvents(options);
}

}   // namespace NCloud::NFileStore::NStorage
