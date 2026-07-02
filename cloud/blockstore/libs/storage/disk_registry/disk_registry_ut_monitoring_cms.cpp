#include "disk_registry.h"

#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/disk_registry/disk_registry_private.h>
#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_env.h>

#include <contrib/ydb/core/testlib/basics/runtime.h>
#include <contrib/ydb/library/actors/core/mon.h>
#include <contrib/ydb/library/actors/protos/actors.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NDiskRegistryTest;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TStorageServiceConfig CreateStorageConfig(bool enableMonpageStateChange)
{
    auto config = CreateDefaultStorageConfig();
    config.SetEnableToChangeStatesFromDiskRegistryMonpage(
        enableMonpageStateChange);
    return config;
}

std::unique_ptr<NMon::TEvRemoteHttpInfo> MakeRequest(
    ui64 tabletId,
    const TString& action,
    const TVector<std::pair<TString, TString>>& params,
    HTTP_METHOD method = HTTP_METHOD_POST)
{
    NActorsProto::TRemoteHttpInfo info;
    info.SetMethod(method);
    info.SetPath("/app");

    auto addQueryParam = [&](const TString& key, const TString& value)
    {
        auto* p = info.AddQueryParams();
        p->SetKey(key);
        p->SetValue(value);
    };

    auto addPostParam = [&](const TString& key, const TString& value)
    {
        auto* p = info.AddPostParams();
        p->SetKey(key);
        p->SetValue(value);
    };

    addQueryParam("TabletID", ToString(tabletId));
    if (method == HTTP_METHOD_POST) {
        addPostParam("action", action);
        addPostParam("TabletID", ToString(tabletId));
        for (const auto& [k, v]: params) {
            addPostParam(k, v);
        }
    } else {
        addQueryParam("action", action);
        for (const auto& [k, v]: params) {
            addQueryParam(k, v);
        }
    }

    return std::make_unique<NMon::TEvRemoteHttpInfo>(info);
}

TString SendAndRecv(
    TTestActorRuntime& runtime,
    const TActorId& sender,
    const TActorId& pipe,
    std::unique_ptr<NMon::TEvRemoteHttpInfo> request)
{
    runtime.SendToPipe(pipe, sender, request.release(), 0, 0);

    TAutoPtr<IEventHandle> handle;
    auto response = runtime.GrabEdgeEventRethrow<NMon::TEvRemoteHttpInfoRes>(
        handle,
        WaitTimeout);
    UNIT_ASSERT(response);
    return response->Html;
}

struct TFixture: public NUnitTest::TBaseFixture
{
    std::unique_ptr<TDiskRegistryTestRuntime> Runtime;
    std::optional<TDiskRegistryClient> DiskRegistry;
    TActorId Sender;
    TActorId PipeClient;

    void Setup(bool enableMonpageStateChange)
    {
        const auto agent = CreateAgentConfig(
            "agent-1",
            {
                Device("dev-1", "uuid-1", "rack-1", 10_GB),
                Device("dev-2", "uuid-2", "rack-1", 10_GB),
            });

        auto storageConfig = std::make_shared<TStorageConfig>(
            CreateStorageConfig(enableMonpageStateChange),
            std::make_shared<NFeatures::TFeaturesConfig>(
                NCloud::NProto::TFeaturesConfig()));

        Runtime = TTestRuntimeBuilder()
                      .WithAgents({agent})
                      .With(storageConfig)
                      .Build();

        DiskRegistry.emplace(*Runtime);
        DiskRegistry->WaitReady();
        DiskRegistry->SetWritableState(true);
        DiskRegistry->UpdateConfig(CreateRegistryConfig(0, {agent}));
        RegisterAndWaitForAgents(*Runtime, {agent});

        Sender = Runtime->AllocateEdgeActor(0);
        PipeClient = Runtime->ConnectToPipe(
            TestTabletId,
            Sender,
            0,
            NKikimr::GetPipeConfigWithRetries());
    }

    TString Send(
        const TString& action,
        const TVector<std::pair<TString, TString>>& params,
        HTTP_METHOD method = HTTP_METHOD_POST)
    {
        return SendAndRecv(
            *Runtime,
            Sender,
            PipeClient,
            MakeRequest(TestTabletId, action, params, method));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskRegistryMonitoringCmsTest)
{
    Y_UNIT_TEST_F(
        ShouldRejectHostRequestWhenMonpageStateChangeDisabled,
        TFixture)
    {
        Setup(false /* enableMonpageStateChange */);

        auto html = Send(
            "sendCmsHostRequest",
            {
                {"AgentID", "agent-1"},
                {"CmsAction",
                 ToString(static_cast<ui32>(NProto::TAction::REMOVE_HOST))},
            });

        UNIT_ASSERT_C(
            html.Contains("Can't send CMS request from monpage"),
            html);
    }

    Y_UNIT_TEST_F(
        ShouldRejectDeviceRequestWhenMonpageStateChangeDisabled,
        TFixture)
    {
        Setup(false /* enableMonpageStateChange */);

        auto html = Send(
            "sendCmsDeviceRequest",
            {
                {"AgentID", "agent-1"},
                {"DeviceName", "dev-1"},
                {"CmsAction",
                 ToString(static_cast<ui32>(NProto::TAction::REMOVE_DEVICE))},
            });

        UNIT_ASSERT_C(
            html.Contains("Can't send CMS request from monpage"),
            html);
    }

    Y_UNIT_TEST_F(ShouldRejectHostRequestWithoutCmsAction, TFixture)
    {
        Setup(true /* enableMonpageStateChange */);

        auto html = Send(
            "sendCmsHostRequest",
            {
                {"AgentID", "agent-1"},
            });

        UNIT_ASSERT_C(html.Contains("No CMS request is given"), html);
    }

    Y_UNIT_TEST_F(ShouldRejectHostRequestWithoutAgentId, TFixture)
    {
        Setup(true /* enableMonpageStateChange */);

        auto html = Send(
            "sendCmsHostRequest",
            {
                {"CmsAction",
                 ToString(static_cast<ui32>(NProto::TAction::REMOVE_HOST))},
            });

        UNIT_ASSERT_C(html.Contains("No agent id is given"), html);
    }

    Y_UNIT_TEST_F(ShouldRejectHostRequestWithUnparseableAction, TFixture)
    {
        Setup(true /* enableMonpageStateChange */);

        auto html = Send(
            "sendCmsHostRequest",
            {
                {"AgentID", "agent-1"},
                {"CmsAction", "not-a-number"},
            });

        UNIT_ASSERT_C(html.Contains("Could not parse CMS request type"), html);
    }

    Y_UNIT_TEST_F(ShouldRejectHostRequestWithDeviceActionType, TFixture)
    {
        Setup(true /* enableMonpageStateChange */);

        auto html = Send(
            "sendCmsHostRequest",
            {
                {"AgentID", "agent-1"},
                {"CmsAction",
                 ToString(static_cast<ui32>(NProto::TAction::ADD_DEVICE))},
            });

        UNIT_ASSERT_C(html.Contains("Invalid CMS request type"), html);
    }

    Y_UNIT_TEST_F(ShouldRejectDeviceRequestWithoutDeviceName, TFixture)
    {
        Setup(true /* enableMonpageStateChange */);

        auto html = Send(
            "sendCmsDeviceRequest",
            {
                {"AgentID", "agent-1"},
                {"CmsAction",
                 ToString(static_cast<ui32>(NProto::TAction::REMOVE_DEVICE))},
            });

        UNIT_ASSERT_C(html.Contains("No device name is given"), html);
    }

    Y_UNIT_TEST_F(ShouldRejectDeviceRequestWithoutAgentId, TFixture)
    {
        Setup(true /* enableMonpageStateChange */);

        auto html = Send(
            "sendCmsDeviceRequest",
            {
                {"DeviceName", "dev-1"},
                {"CmsAction",
                 ToString(static_cast<ui32>(NProto::TAction::REMOVE_DEVICE))},
            });

        UNIT_ASSERT_C(html.Contains("No agent id is given"), html);
    }

    Y_UNIT_TEST_F(ShouldRejectDeviceRequestWithHostActionType, TFixture)
    {
        Setup(true /* enableMonpageStateChange */);

        auto html = Send(
            "sendCmsDeviceRequest",
            {
                {"AgentID", "agent-1"},
                {"DeviceName", "dev-1"},
                {"CmsAction",
                 ToString(static_cast<ui32>(NProto::TAction::REMOVE_HOST))},
            });

        UNIT_ASSERT_C(html.Contains("Invalid CMS request type"), html);
    }

    Y_UNIT_TEST_F(ShouldRejectRequestWithWrongHttpMethod, TFixture)
    {
        Setup(true /* enableMonpageStateChange */);

        auto html = Send(
            "sendCmsHostRequest",
            {
                {"AgentID", "agent-1"},
                {"CmsAction",
                 ToString(static_cast<ui32>(NProto::TAction::REMOVE_HOST))},
            },
            HTTP_METHOD_GET);

        UNIT_ASSERT_C(html.Contains("Wrong HTTP method"), html);
    }

    Y_UNIT_TEST_F(ShouldEmitUpdateCmsHostStateForAddHost, TFixture)
    {
        Setup(true /* enableMonpageStateChange */);

        std::optional<TEvDiskRegistryPrivate::TUpdateCmsHostStateRequest>
            captured;
        Runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvDiskRegistryPrivate::EvUpdateCmsHostStateRequest)
                {
                    auto* msg = event->Get<
                        TEvDiskRegistryPrivate::TEvUpdateCmsHostStateRequest>();
                    captured.emplace(msg->Host, msg->State, msg->DryRun);
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        auto html = Send(
            "sendCmsHostRequest",
            {
                {"AgentID", "agent-1"},
                {"CmsAction",
                 ToString(static_cast<ui32>(NProto::TAction::ADD_HOST))},
                {"DryRun", "1"},
            });

        UNIT_ASSERT_C(captured.has_value(), "no UpdateCmsHostState request");
        UNIT_ASSERT_VALUES_EQUAL("agent-1", captured->Host);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::AGENT_STATE_ONLINE),
            static_cast<int>(captured->State));
        UNIT_ASSERT(captured->DryRun);
        UNIT_ASSERT_C(html.Contains("CMS request ADD_HOST"), html);
    }

    Y_UNIT_TEST_F(ShouldEmitUpdateCmsHostStateForRemoveHost, TFixture)
    {
        Setup(true /* enableMonpageStateChange */);

        std::optional<TEvDiskRegistryPrivate::TUpdateCmsHostStateRequest>
            captured;
        Runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvDiskRegistryPrivate::EvUpdateCmsHostStateRequest)
                {
                    auto* msg = event->Get<
                        TEvDiskRegistryPrivate::TEvUpdateCmsHostStateRequest>();
                    captured.emplace(msg->Host, msg->State, msg->DryRun);
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        auto html = Send(
            "sendCmsHostRequest",
            {
                {"AgentID", "agent-1"},
                {"CmsAction",
                 ToString(static_cast<ui32>(NProto::TAction::REMOVE_HOST))},
            });

        UNIT_ASSERT_C(captured.has_value(), "no UpdateCmsHostState request");
        UNIT_ASSERT_VALUES_EQUAL("agent-1", captured->Host);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::AGENT_STATE_WARNING),
            static_cast<int>(captured->State));
        UNIT_ASSERT(!captured->DryRun);
        UNIT_ASSERT_C(html.Contains("CMS request REMOVE_HOST"), html);
    }

    Y_UNIT_TEST_F(ShouldEmitPurgeHostCmsForPurgeHost, TFixture)
    {
        Setup(true /* enableMonpageStateChange */);

        std::optional<TEvDiskRegistryPrivate::TPurgeHostCmsRequest> captured;
        Runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvDiskRegistryPrivate::EvPurgeHostCmsRequest)
                {
                    auto* msg = event->Get<
                        TEvDiskRegistryPrivate::TEvPurgeHostCmsRequest>();
                    captured.emplace(msg->Host, msg->DryRun);
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        auto html = Send(
            "sendCmsHostRequest",
            {
                {"AgentID", "agent-1"},
                {"CmsAction",
                 ToString(static_cast<ui32>(NProto::TAction::PURGE_HOST))},
                {"DryRun", "1"},
            });

        UNIT_ASSERT_C(captured.has_value(), "no PurgeHostCms request");
        UNIT_ASSERT_VALUES_EQUAL("agent-1", captured->Host);
        UNIT_ASSERT(captured->DryRun);
        UNIT_ASSERT_C(html.Contains("CMS request PURGE_HOST"), html);
    }

    Y_UNIT_TEST_F(ShouldEmitUpdateCmsHostDeviceStateForAddDevice, TFixture)
    {
        Setup(true /* enableMonpageStateChange */);

        std::optional<TEvDiskRegistryPrivate::TUpdateCmsHostDeviceStateRequest>
            captured;
        Runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvDiskRegistryPrivate::EvUpdateCmsHostDeviceStateRequest)
                {
                    auto* msg =
                        event->Get<TEvDiskRegistryPrivate::
                                       TEvUpdateCmsHostDeviceStateRequest>();
                    captured.emplace(
                        msg->Host,
                        msg->Path,
                        msg->State,
                        msg->ShouldResumeDevice,
                        msg->DryRun);
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        auto html = Send(
            "sendCmsDeviceRequest",
            {
                {"AgentID", "agent-1"},
                {"DeviceName", "dev-1"},
                {"CmsAction",
                 ToString(static_cast<ui32>(NProto::TAction::ADD_DEVICE))},
                {"DryRun", "1"},
            });

        UNIT_ASSERT_C(
            captured.has_value(),
            "no UpdateCmsHostDeviceState request");
        UNIT_ASSERT_VALUES_EQUAL("agent-1", captured->Host);
        UNIT_ASSERT_VALUES_EQUAL("dev-1", captured->Path);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::DEVICE_STATE_ONLINE),
            static_cast<int>(captured->State));
        UNIT_ASSERT(!captured->ShouldResumeDevice);
        UNIT_ASSERT(captured->DryRun);
        UNIT_ASSERT_C(html.Contains("CMS request ADD_DEVICE"), html);
    }

    Y_UNIT_TEST_F(ShouldEmitUpdateCmsHostDeviceStateForRemoveDevice, TFixture)
    {
        Setup(true /* enableMonpageStateChange */);

        std::optional<TEvDiskRegistryPrivate::TUpdateCmsHostDeviceStateRequest>
            captured;
        Runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvDiskRegistryPrivate::EvUpdateCmsHostDeviceStateRequest)
                {
                    auto* msg =
                        event->Get<TEvDiskRegistryPrivate::
                                       TEvUpdateCmsHostDeviceStateRequest>();
                    captured.emplace(
                        msg->Host,
                        msg->Path,
                        msg->State,
                        msg->ShouldResumeDevice,
                        msg->DryRun);
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        auto html = Send(
            "sendCmsDeviceRequest",
            {
                {"AgentID", "agent-1"},
                {"DeviceName", "dev-1"},
                {"CmsAction",
                 ToString(static_cast<ui32>(NProto::TAction::REMOVE_DEVICE))},
            });

        UNIT_ASSERT_C(
            captured.has_value(),
            "no UpdateCmsHostDeviceState request");
        UNIT_ASSERT_VALUES_EQUAL("agent-1", captured->Host);
        UNIT_ASSERT_VALUES_EQUAL("dev-1", captured->Path);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::DEVICE_STATE_WARNING),
            static_cast<int>(captured->State));
        UNIT_ASSERT(!captured->ShouldResumeDevice);
        UNIT_ASSERT(!captured->DryRun);
        UNIT_ASSERT_C(html.Contains("CMS request REMOVE_DEVICE"), html);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
