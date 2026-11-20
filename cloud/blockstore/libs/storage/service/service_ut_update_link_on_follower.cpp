#include "service_ut.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/testlib/test_runtime.h>

#include <google/protobuf/util/json_util.h>

#include <util/generic/ylimits.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TUpdateLinkOnFollowerRequest MakeUpdateLinkRequest()
{
    NProto::TUpdateLinkOnFollowerRequest request;
    request.SetDiskId(DefaultDiskId);
    request.SetLinkUUID("link-uuid");
    // The same disk id in different cells is a valid full address.
    request.SetLeaderDiskId(DefaultDiskId);
    request.SetLeaderCellId("leader-cell");
    request.SetFollowerCellId("follower-cell");
    return request;
}

TString ToJson(const NProto::TUpdateLinkOnFollowerRequest& request)
{
    TString input;
    UNIT_ASSERT(
        google::protobuf::util::MessageToJsonString(request, &input).ok());
    return input;
}

struct TFixture
{
    TTestEnv Env;
    const ui32 NodeIndex = SetupTestEnvWithCellId(Env, "follower-cell");
    TTestActorRuntime& Runtime = Env.GetRuntime();
    TServiceClient Service{Runtime, NodeIndex};

    void Send(const NProto::TUpdateLinkOnFollowerRequest& record)
    {
        Service.SendExecuteActionRequest(
            "UpdateLinkOnFollower",
            ToJson(record));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceUpdateLinkOnFollowerTest)
{
    Y_UNIT_TEST(ShouldValidateInputBeforeSendingToVolumeProxy)
    {
        TFixture f;
        ui32 updates = 0;
        auto observer =
            f.Runtime.AddObserver<TEvVolume::TEvUpdateLinkOnFollowerRequest>(
                [&](auto&) { ++updates; });
        TVector<TString> inputs = {"{", "{}"};
        for (ui32 field = 0; field != 7; ++field) {
            auto request = MakeUpdateLinkRequest();
            switch (field) {
                case 0: request.ClearDiskId(); break;
                case 1: request.ClearLinkUUID(); break;
                case 2: request.ClearLeaderDiskId(); break;
                case 3: request.ClearLeaderCellId(); break;
                case 4: request.ClearFollowerCellId(); break;
                case 5: request.SetFollowerCellId("wrong-cell"); break;
                case 6: request.SetLeaderCellId("follower-cell"); break;
            }
            inputs.push_back(ToJson(request));
        }
        for (const auto action:
             {NProto::LINK_ACTION_DESTROY, NProto::LINK_ACTION_COMPLETED})
        {
            auto request = MakeUpdateLinkRequest();
            request.SetAction(action);
            request.ClearLinkUUID();
            inputs.push_back(ToJson(request));
        }
        for (const TString action: {"99", "-1", "\"UNKNOWN\""}) {
            auto input = ToJson(MakeUpdateLinkRequest());
            input.pop_back();
            input += ",\"Action\":" + action + "}";
            inputs.push_back(std::move(input));
        }
        for (const auto& input: inputs) {
            f.Service.SendExecuteActionRequest("updatelinkonfollower", input);
            const auto response = f.Service.RecvExecuteActionResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                response->GetStatus(),
                input);
            UNIT_ASSERT(response->Record.GetOutput().empty());
        }
        UNIT_ASSERT_VALUES_EQUAL(0, updates);
    }

    Y_UNIT_TEST(ShouldSanitizeHeadersAndPreserveVolumeResult)
    {
        TFixture f;
        const auto callContext = MakeIntrusive<TCallContext>(ui64{123});
        ui32 updates = 0;
        NProto::TError result;
        auto sentRequest = MakeUpdateLinkRequest();
        f.Runtime.SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() !=
                    TEvVolume::EvUpdateLinkOnFollowerRequest)
                {
                    return false;
                }
                ++updates;
                const auto* msg =
                    event->Get<TEvVolume::TEvUpdateLinkOnFollowerRequest>();
                UNIT_ASSERT(msg->CallContext == callContext);
                auto expected = sentRequest;
                expected.MutableHeaders()->SetExactDiskIdMatch(true);
                expected.MutableHeaders()->SetRequestId(123);
                UNIT_ASSERT_VALUES_EQUAL(
                    expected.SerializeAsString(),
                    msg->Record.SerializeAsString());
                f.Runtime.SendAsync(
                    new IEventHandle(
                        event->Sender,
                        event->Recipient,
                        new TEvVolume::TEvUpdateLinkOnFollowerResponse(result),
                        0,
                        event->Cookie),
                    f.NodeIndex);
                return true;
            });

        for (const ui32 code: {S_OK, S_ALREADY, S_FALSE, E_REJECTED}) {
            result = MakeError(code, "original volume result");
            result.SetFlags(7);
            sentRequest.SetAction(
                static_cast<NProto::ELinkAction>(updates % 3));
            auto request = sentRequest;
            auto* headers = request.MutableHeaders();
            headers->SetRequestId(999);
            headers->SetCellId("forged-cell");
            headers->SetClientId("forged-client");
            headers->SetExactDiskIdMatch(false);
            headers->MutableInternal()->SetRequestSource(
                NCloud::NProto::SOURCE_SECURE_CONTROL_CHANNEL);
            headers->MutableInternal()->SetPeer("forged-peer");
            auto action = f.Service.CreateExecuteActionRequest(
                "updatelinkonfollower",
                ToJson(request));
            action->CallContext = callContext;
            f.Service.SendRequest(MakeStorageServiceId(), std::move(action));
            const auto response = f.Service.RecvExecuteActionResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                result.SerializeAsString(),
                response->GetError().SerializeAsString());
            UNIT_ASSERT(response->Record.GetOutput().empty());
        }
        UNIT_ASSERT_VALUES_EQUAL(4, updates);
    }

    Y_UNIT_TEST(ShouldUpdateActualFollowerAndReturnIdempotentResult)
    {
        TFixture f;
        f.Service.CreateVolume();
        auto request = MakeUpdateLinkRequest();
        // The standard serializer omits CREATE because it is the default.
        f.Send(request);
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            f.Service.RecvExecuteActionResponse()->GetStatus());
        f.Send(request);
        UNIT_ASSERT_VALUES_EQUAL(
            S_ALREADY,
            f.Service.RecvExecuteActionResponse()->GetStatus());
        request.SetAction(NProto::LINK_ACTION_DESTROY);
        f.Send(request);
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            f.Service.RecvExecuteActionResponse()->GetStatus());
        f.Send(request);
        UNIT_ASSERT_VALUES_EQUAL(
            S_ALREADY,
            f.Service.RecvExecuteActionResponse()->GetStatus());
    }

    Y_UNIT_TEST(ShouldReturnVolumeProxyError)
    {
        TFixture f;
        f.Send(MakeUpdateLinkRequest());
        const auto response = f.Service.RecvExecuteActionResponse();
        UNIT_ASSERT(IsNotFoundSchemeShardError(response->GetError()));
        UNIT_ASSERT(response->Record.GetOutput().empty());
    }

    Y_UNIT_TEST(ShouldRejectInitialUndeliveryToVolumeProxy)
    {
        TFixture f;
        // Use a non-edge mailbox: the runtime holds all events addressed to
        // an edge mailbox without attempting delivery to the local actor id.
        const auto volumeProxy =
            f.Runtime.GetActorSystem(f.NodeIndex)
                ->LookupLocalService(MakeVolumeProxyServiceId());
        UNIT_ASSERT(volumeProxy);
        const TActorId missingActor(
            volumeProxy.NodeId(),
            volumeProxy.PoolID(),
            Max<ui64>(),
            volumeProxy.Hint());
        f.Runtime.RegisterService(
            MakeVolumeProxyServiceId(),
            missingActor,
            f.NodeIndex);

        TActorId actionActor;
        const TEvVolume::TEvUpdateLinkOnFollowerRequest* original = nullptr;
        ui32 requests = 0;
        auto observer =
            f.Runtime.AddObserver<TEvVolume::TEvUpdateLinkOnFollowerRequest>(
                [&](auto& event)
                {
                    ++requests;
                    if (requests == 1) {
                        actionActor = event->Sender;
                        original = event->Get();
                        UNIT_ASSERT_VALUES_EQUAL(
                            MakeVolumeProxyServiceId(),
                            event->Recipient);
                        UNIT_ASSERT_VALUES_EQUAL(
                            missingActor,
                            event->GetRecipientRewrite());
                        UNIT_ASSERT(
                            event->Flags &
                            IEventHandle::FlagForwardOnNondelivery);
                        UNIT_ASSERT_VALUES_EQUAL(
                            actionActor,
                            event->GetForwardOnNondeliveryRecipient());
                    } else {
                        UNIT_ASSERT_VALUES_EQUAL(2, requests);
                        UNIT_ASSERT(original == event->Get());
                        UNIT_ASSERT_VALUES_EQUAL(actionActor, event->Recipient);
                        UNIT_ASSERT_VALUES_EQUAL(
                            actionActor,
                            event->GetRecipientRewrite());
                        UNIT_ASSERT(
                            !(event->Flags &
                              IEventHandle::FlagForwardOnNondelivery));
                        UNIT_ASSERT_VALUES_EQUAL(
                            MakeVolumeProxyServiceId(),
                            event->GetForwardOnNondeliveryRecipient());
                    }
                });
        f.Send(MakeUpdateLinkRequest());
        const auto response = f.Service.RecvExecuteActionResponse();
        UNIT_ASSERT_VALUES_EQUAL(2, requests);
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        UNIT_ASSERT(response->Record.GetOutput().empty());
    }

    Y_UNIT_TEST(ShouldWaitForVolumeResponseAndTimeoutWhenItIsMissing)
    {
        TFixture f;
        ui32 updates = 0;
        f.Runtime.SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvVolume::EvUpdateLinkOnFollowerRequest)
                {
                    ++updates;
                    return true;
                }
                return false;
            });
        std::optional<TInstant> responseTime;
        auto observer =
            f.Runtime.AddObserver<TEvService::TEvExecuteActionResponse>(
                [&](auto& event)
                {
                    if (event->Recipient == f.Service.GetSender()) {
                        responseTime = f.Runtime.GetCurrentTime();
                    }
                });
        const auto timer = f.Runtime.AllocateEdgeActor(f.NodeIndex);
        const auto dispatchUntil = [&](TInstant deadline)
        {
            f.Runtime.Schedule(
                new IEventHandle(timer, timer, new TEvents::TEvWakeup()),
                deadline - f.Runtime.GetCurrentTime(),
                f.NodeIndex);
            UNIT_ASSERT(
                f.Runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(timer));
            UNIT_ASSERT_VALUES_EQUAL(deadline, f.Runtime.GetCurrentTime());
        };
        const auto deadline =
            f.Runtime.GetCurrentTime() + TDuration::Seconds(30);
        f.Send(MakeUpdateLinkRequest());
        dispatchUntil(deadline - TDuration::MilliSeconds(1));
        UNIT_ASSERT_C(
            !responseTime,
            "Action replied before receiving a result");
        dispatchUntil(deadline + TDuration::MilliSeconds(1));
        UNIT_ASSERT_C(responseTime, "Action did not time out");
        UNIT_ASSERT_VALUES_EQUAL(deadline, *responseTime);
        UNIT_ASSERT_VALUES_EQUAL(1, updates);
        const auto response = f.Service.RecvExecuteActionResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, response->GetStatus());
        UNIT_ASSERT(response->Record.GetOutput().empty());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
