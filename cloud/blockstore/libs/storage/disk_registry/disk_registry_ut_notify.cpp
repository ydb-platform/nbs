#include "disk_registry.h"
#include "disk_registry_actor.h"

#include <cloud/blockstore/config/disk.pb.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/notify/iface/notify.h>
#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_env.h>
#include <cloud/blockstore/libs/storage/testlib/ss_proxy_client.h>

#include <contrib/ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/type_name.h>

#include <chrono>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NDiskRegistryTest;

using namespace NThreading;

using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TFakeNotifyService
    : public NNotify::IService
{
public:
    TVector<NNotify::TNotification> Requests;

    NProto::TError Error;

public:
    TFuture<NProto::TError> Notify(
        const NNotify::TNotification& data) override
    {
        Requests.push_back(std::move(data));

        return MakeFuture(Error);
    }

    void Start() override
    {}

    void Stop() override
    {}
};

////////////////////////////////////////////////////////////////////////////////

template <typename... Ts>
TString GetAlternativeTypeName(const std::variant<Ts...>& v)
{
    return std::visit(
        [] <typename T> (const T&) { return TypeName<T>(); },
        v);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

#define MY_UNIT_ASSERT_NOTIFICATION_EVENT_FIELD_EQUAL(                         \
            value, event, eventType, field)                                    \
    do {                                                                       \
        UNIT_ASSERT_C(std::holds_alternative<NNotify::eventType>(event),       \
            "expected, but variant holds " << GetAlternativeTypeName(event));  \
        UNIT_ASSERT_VALUES_EQUAL(value,                                        \
            std::get<NNotify::eventType>(event).field);                        \
    } while (false)

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskRegistryTest)
{
    Y_UNIT_TEST(ShouldNotifyAboutDiskError)
    {
        auto notifyService = std::make_shared<TFakeNotifyService>();

        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1", "rack-1", 10_GB),
                Device("dev-2", "uuid-2", "rack-1", 10_GB)
            }),
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .With(notifyService)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, agents));

        RegisterAndWaitForAgents(*runtime, agents);

        diskRegistry.AllocateDisk("nonrepl-vol", 10_GB, 4_KB, "", 0, "", "");

        int notifications = 0;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvDiskRegistryPrivate::EvNotifyUserEventRequest: {
                        ++notifications;
                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        diskRegistry.ChangeAgentState("agent-1", NProto::AGENT_STATE_UNAVAILABLE);

        UNIT_ASSERT_VALUES_EQUAL(1, notifications);
        UNIT_ASSERT_VALUES_EQUAL(0, notifyService->Requests.size());

        diskRegistry.AllocateDisk("nonrepl-vol", 10_GB, 4_KB, "", 0, "yc-nbs", "yc-nbs.folder");

        runtime->AdvanceCurrentTime(5s);
        runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT_VALUES_EQUAL(2, notifications);
        UNIT_ASSERT_VALUES_EQUAL(1, notifyService->Requests.size());
        UNIT_ASSERT_VALUES_EQUAL("yc-nbs", notifyService->Requests[0].CloudId);
        UNIT_ASSERT_VALUES_EQUAL("yc-nbs.folder", notifyService->Requests[0].FolderId);
        MY_UNIT_ASSERT_NOTIFICATION_EVENT_FIELD_EQUAL("nonrepl-vol",
            notifyService->Requests[0].Event, TDiskError, DiskId);
        UNIT_ASSERT_VALUES_EQUAL("", notifyService->Requests[0].UserId);

        runtime->AdvanceCurrentTime(10s);
        runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT_VALUES_EQUAL(2, notifications);
        UNIT_ASSERT_VALUES_EQUAL(1, notifyService->Requests.size());
    }

    Y_UNIT_TEST(ShouldNotifyByUserId)
    {
        auto notifyService = std::make_shared<TFakeNotifyService>();

        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1", "rack-1", 10_GB),
                Device("dev-2", "uuid-2", "rack-1", 10_GB)
            }),
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .With(notifyService)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, agents));

        RegisterAndWaitForAgents(*runtime, agents);

        diskRegistry.AllocateDisk("nonrepl-vol", 10_GB, 4_KB, "", 0, "yc-nbs", "yc-nbs.folder");
        diskRegistry.SetUserId("nonrepl-vol", "vasya");

        diskRegistry.RebootTablet();
        diskRegistry.WaitReady();

        int notifications = 0;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvDiskRegistryPrivate::EvNotifyUserEventRequest: {
                        ++notifications;
                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        diskRegistry.ChangeAgentState("agent-1", NProto::AGENT_STATE_UNAVAILABLE);

        UNIT_ASSERT_VALUES_EQUAL(1, notifications);
        UNIT_ASSERT_VALUES_EQUAL(1, notifyService->Requests.size());
        UNIT_ASSERT_VALUES_EQUAL("yc-nbs", notifyService->Requests[0].CloudId);
        UNIT_ASSERT_VALUES_EQUAL("yc-nbs.folder", notifyService->Requests[0].FolderId);
        MY_UNIT_ASSERT_NOTIFICATION_EVENT_FIELD_EQUAL("nonrepl-vol",
            notifyService->Requests[0].Event, TDiskError, DiskId);
        UNIT_ASSERT_VALUES_EQUAL("vasya", notifyService->Requests[0].UserId);

        runtime->AdvanceCurrentTime(10s);
        runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT_VALUES_EQUAL(1, notifications);
        UNIT_ASSERT_VALUES_EQUAL(1, notifyService->Requests.size());
    }

    Y_UNIT_TEST(ShouldNotifyAboutDiskErrorAfterReboot)
    {
        auto notifyService = std::make_shared<TFakeNotifyService>();

        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1", "rack-1", 10_GB),
                Device("dev-2", "uuid-2", "rack-1", 10_GB)
            }),
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .With(notifyService)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, agents));

        RegisterAndWaitForAgents(*runtime, agents);

        diskRegistry.AllocateDisk("nonrepl-vol", 10_GB, 4_KB, "", 0, "", "");

        diskRegistry.ChangeAgentState("agent-1", NProto::AGENT_STATE_UNAVAILABLE);
        UNIT_ASSERT_VALUES_EQUAL(0, notifyService->Requests.size());

        diskRegistry.AllocateDisk("nonrepl-vol", 10_GB, 4_KB, "", 0, "yc-nbs", "yc-nbs.folder");
        UNIT_ASSERT_VALUES_EQUAL(0, notifyService->Requests.size());

        diskRegistry.RebootTablet();
        diskRegistry.WaitReady();

        UNIT_ASSERT_VALUES_EQUAL(1, notifyService->Requests.size());
        UNIT_ASSERT_VALUES_EQUAL("yc-nbs", notifyService->Requests[0].CloudId);
        UNIT_ASSERT_VALUES_EQUAL("yc-nbs.folder", notifyService->Requests[0].FolderId);
        MY_UNIT_ASSERT_NOTIFICATION_EVENT_FIELD_EQUAL("nonrepl-vol",
            notifyService->Requests[0].Event, TDiskError, DiskId);

        runtime->AdvanceCurrentTime(10s);
        runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT_VALUES_EQUAL(1, notifyService->Requests.size());
    }

    Y_UNIT_TEST(ShouldRetryNotifications)
    {
        auto notifyService = std::make_shared<TFakeNotifyService>();
        notifyService->Error = MakeError(E_REJECTED);

        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1", "rack-1", 10_GB),
            }),
            CreateAgentConfig("agent-2", {
                Device("dev-1", "uuid-2", "rack-1", 10_GB),
            }),
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .With(notifyService)
            .Build();

        NMonitoring::TDynamicCountersPtr counters = new NMonitoring::TDynamicCounters();
        InitCriticalEventsCounter(counters);
        auto userNotificationError =
            counters->GetCounter("AppCriticalEvents/UserNotificationError", true);

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        UNIT_ASSERT_VALUES_EQUAL(0, userNotificationError->Val());

        diskRegistry.UpdateConfig(CreateRegistryConfig(agents));

        RegisterAndWaitForAgents(*runtime, agents);

        diskRegistry.AllocateDisk("nonrepl-vol-1", 10_GB, 4_KB, "", 0, "yc-nbs", "foo");
        diskRegistry.AllocateDisk("nonrepl-vol-2", 10_GB, 4_KB, "", 0, "yc-nbs", "bar");

        UNIT_ASSERT_VALUES_EQUAL(0, notifyService->Requests.size());

        diskRegistry.ChangeAgentState("agent-1", NProto::AGENT_STATE_UNAVAILABLE);

        UNIT_ASSERT_VALUES_EQUAL(1, notifyService->Requests.size());
        MY_UNIT_ASSERT_NOTIFICATION_EVENT_FIELD_EQUAL("nonrepl-vol-1",
            notifyService->Requests[0].Event, TDiskError, DiskId);

        runtime->AdvanceCurrentTime(5s);
        runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT_VALUES_EQUAL(2, notifyService->Requests.size());
        MY_UNIT_ASSERT_NOTIFICATION_EVENT_FIELD_EQUAL("nonrepl-vol-1",
            notifyService->Requests[1].Event, TDiskError, DiskId);

        UNIT_ASSERT_VALUES_EQUAL(0, userNotificationError->Val());

        notifyService->Error = MakeError(S_OK);

        runtime->AdvanceCurrentTime(10s);
        runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT_VALUES_EQUAL(3, notifyService->Requests.size());
        MY_UNIT_ASSERT_NOTIFICATION_EVENT_FIELD_EQUAL("nonrepl-vol-1",
            notifyService->Requests[2].Event, TDiskError, DiskId);

        notifyService->Error = MakeError(E_REJECTED);

        diskRegistry.ChangeAgentState("agent-2", NProto::AGENT_STATE_UNAVAILABLE);
        runtime->AdvanceCurrentTime(5s);
        runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT_VALUES_EQUAL(4, notifyService->Requests.size());
        MY_UNIT_ASSERT_NOTIFICATION_EVENT_FIELD_EQUAL("nonrepl-vol-2",
            notifyService->Requests[3].Event, TDiskError, DiskId);

        runtime->AdvanceCurrentTime(5s);
        runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT_VALUES_EQUAL(5, notifyService->Requests.size());
        MY_UNIT_ASSERT_NOTIFICATION_EVENT_FIELD_EQUAL("nonrepl-vol-2",
            notifyService->Requests[4].Event, TDiskError, DiskId);

        UNIT_ASSERT_VALUES_EQUAL(0, userNotificationError->Val());
        notifyService->Error = MakeError(E_FAIL);

        runtime->AdvanceCurrentTime(5s);
        runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT_VALUES_EQUAL(6, notifyService->Requests.size());
        MY_UNIT_ASSERT_NOTIFICATION_EVENT_FIELD_EQUAL("nonrepl-vol-2",
            notifyService->Requests[5].Event, TDiskError, DiskId);
        UNIT_ASSERT_VALUES_EQUAL(1, userNotificationError->Val());

        runtime->AdvanceCurrentTime(5s);
        runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT_VALUES_EQUAL(6, notifyService->Requests.size());
        UNIT_ASSERT_VALUES_EQUAL(1, userNotificationError->Val());

        diskRegistry.RebootTablet();
        diskRegistry.WaitReady();

        UNIT_ASSERT_VALUES_EQUAL(6, notifyService->Requests.size());
        UNIT_ASSERT_VALUES_EQUAL(1, userNotificationError->Val());

        runtime->AdvanceCurrentTime(5s);
        runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT_VALUES_EQUAL(6, notifyService->Requests.size());
        UNIT_ASSERT_VALUES_EQUAL(1, userNotificationError->Val());
    }

    Y_UNIT_TEST(ShouldIgnoreDeletedDisk)
    {
        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1", "rack-1", 10_GB),
            })
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, agents));

        RegisterAndWaitForAgents(*runtime, agents);

        TAutoPtr<IEventHandle> delayedRequest;
        runtime->SetEventFilter(
            [&] (TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
                switch (ev->GetTypeRewrite()) {
                    case TEvDiskRegistryPrivate::EvPublishDiskStatesRequest: {
                        if (!delayedRequest) {
                            delayedRequest = ev.Release();
                            return true;
                        }
                        return false;
                    }
                }

                return false;
            });

        diskRegistry.AllocateDisk("vol0", 10_GB, 4_KB, "", 0, "", "");
        diskRegistry.ChangeAgentState("agent-1", NProto::AGENT_STATE_UNAVAILABLE);

        UNIT_ASSERT(delayedRequest);

        diskRegistry.MarkDiskForCleanup("vol0");
        diskRegistry.DeallocateDisk(
            "vol0",
            false); // sync

        // resend event
        runtime->DispatchEvents({}, 10ms);
        runtime->Send(delayedRequest.Release());
        runtime->DispatchEvents({}, 10ms);
    }

    Y_UNIT_TEST(ShouldNotifyAboutReturningDiskBackOnline)
    {
        auto notifyService = std::make_shared<TFakeNotifyService>();

        const TVector agents {
            CreateAgentConfig("agent-1", {
                Device("dev-1", "uuid-1", "rack-1", 10_GB),
                Device("dev-2", "uuid-2", "rack-1", 10_GB),
            }),
        };

        auto runtime = TTestRuntimeBuilder()
            .WithAgents(agents)
            .With(notifyService)
            .Build();

        TDiskRegistryClient diskRegistry(*runtime);
        diskRegistry.WaitReady();
        diskRegistry.SetWritableState(true);

        diskRegistry.UpdateConfig(CreateRegistryConfig(0, agents));

        RegisterAndWaitForAgents(*runtime, agents);

        diskRegistry.AllocateDisk("nonrepl-vol", 10_GB, 4_KB, "", 0, "", "");

        int notifications = 0;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvDiskRegistryPrivate::EvNotifyUserEventRequest: {
                        ++notifications;
                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        diskRegistry.ChangeAgentState("agent-1", NProto::AGENT_STATE_UNAVAILABLE);
        diskRegistry.ChangeAgentState("agent-1", NProto::AGENT_STATE_ONLINE);
        diskRegistry.ChangeAgentState("agent-1", NProto::AGENT_STATE_UNAVAILABLE);
        diskRegistry.ChangeAgentState("agent-1", NProto::AGENT_STATE_ONLINE);
        diskRegistry.ChangeAgentState("agent-1", NProto::AGENT_STATE_UNAVAILABLE);

        UNIT_ASSERT_VALUES_EQUAL(1, notifications);
        UNIT_ASSERT_VALUES_EQUAL(0, notifyService->Requests.size());

        runtime->AdvanceCurrentTime(5s);
        runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT_VALUES_EQUAL(6, notifications);
        UNIT_ASSERT_VALUES_EQUAL(0, notifyService->Requests.size());

        diskRegistry.AllocateDisk("nonrepl-vol", 10_GB, 4_KB, "", 0, "yc-nbs", "yc-nbs.folder");

        runtime->AdvanceCurrentTime(5s);
        runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT_VALUES_EQUAL(11, notifications);
        UNIT_ASSERT_VALUES_EQUAL(5, notifyService->Requests.size());
        UNIT_ASSERT_VALUES_EQUAL("yc-nbs", notifyService->Requests[1].CloudId);
        UNIT_ASSERT_VALUES_EQUAL("yc-nbs.folder", notifyService->Requests[1].FolderId);
        MY_UNIT_ASSERT_NOTIFICATION_EVENT_FIELD_EQUAL("nonrepl-vol",
            notifyService->Requests[1].Event, TDiskBackOnline, DiskId);
        UNIT_ASSERT_VALUES_EQUAL("", notifyService->Requests[1].UserId);

        runtime->AdvanceCurrentTime(10s);
        runtime->DispatchEvents({}, 10ms);

        UNIT_ASSERT_VALUES_EQUAL(11, notifications);
        UNIT_ASSERT_VALUES_EQUAL(5, notifyService->Requests.size());

        diskRegistry.ChangeAgentState("agent-1", NProto::AGENT_STATE_ONLINE);
        diskRegistry.ChangeAgentState("agent-1", NProto::AGENT_STATE_UNAVAILABLE);
        diskRegistry.ChangeAgentState("agent-1", NProto::AGENT_STATE_ONLINE);
        diskRegistry.ChangeAgentState("agent-1", NProto::AGENT_STATE_UNAVAILABLE);
        diskRegistry.ChangeAgentState("agent-1", NProto::AGENT_STATE_ONLINE);

        notifyService->Requests.clear();
        diskRegistry.RebootTablet();
        diskRegistry.WaitReady();

        UNIT_ASSERT_VALUES_EQUAL(4, notifyService->Requests.size());
        auto backOnlineCount = CountIf(notifyService->Requests, [] (const auto& n) {
            return std::holds_alternative<NNotify::TDiskBackOnline>(n.Event);
        });
        UNIT_ASSERT_VALUES_EQUAL(2, backOnlineCount);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
