#include <library/cpp/testing/unittest/registar.h>
#include <contrib/ydb/core/testlib/test_client.h>
#include <contrib/ydb/core/testlib/tenant_runtime.h>
#include <contrib/ydb/core/tx/schemeshard/ut_helpers/test_env.h>
#include <contrib/ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <contrib/ydb/core/graph/api/service.h>
#include <contrib/ydb/core/graph/api/events.h>

#ifdef NDEBUG
#define Ctest Cnull
#else
#define Ctest Cerr
#endif

namespace NKikimr {

using namespace Tests;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(Graph) {
    TTenantTestConfig GetTenantTestConfig() {
        return {
            .Domains = {
                {
                    .Name = DOMAIN1_NAME,
                    .SchemeShardId = SCHEME_SHARD1_ID,
                    .Subdomains = {TENANT1_1_NAME, TENANT1_2_NAME}
                }
            },
            .HiveId = HIVE_ID,
            .FakeTenantSlotBroker = true,
            .FakeSchemeShard = true,
            .CreateConsole = false,
            .Nodes = {
                {
                    .TenantPoolConfig = {
                        .StaticSlots = {
                            {
                                .Tenant = DOMAIN1_NAME,
                                .Limit = {
                                    .CPU = 1,
                                    .Memory = 1,
                                    .Network = 1
                                }
                            }
                        },
                        .NodeType = "node-type"
                    }
                }
            },
            .DataCenterCount = 1
        };
    }



    Y_UNIT_TEST(CreateGraphShard) {
        TTestBasicRuntime runtime;

        runtime.SetLogPriority(NKikimrServices::GRAPH, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::HIVE, NLog::PRI_TRACE);

        TTestEnv env(runtime);
        ui64 txId = 100;
        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
            Name: "db1"
        )");

        env.TestWaitNotification(runtime, txId);

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
            Name: "db1"
            PlanResolution: 50
            Coordinators: 1
            Mediators: 1
            TimeCastBucketsPerMediator: 2
            StoragePools {
                Name: "pool-1"
                Kind: "hdd"
            }
            ExternalSchemeShard: true
            ExternalHive: true
            GraphShard: true
        )");

        env.TestWaitNotification(runtime, txId);

        auto result = DescribePath(runtime, "/MyRoot/db1");
        UNIT_ASSERT(result.GetPathDescription().GetDomainDescription().GetProcessingParams().GetGraphShard() != 0);
    }

    Y_UNIT_TEST(UseGraphShard) {
        TTestBasicRuntime runtime;

        runtime.SetLogPriority(NKikimrServices::GRAPH, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::HIVE, NLog::PRI_TRACE);

        TTestEnv::ENABLE_SCHEMESHARD_LOG = false;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
            Name: "db1"
        )");

        env.TestWaitNotification(runtime, txId);

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
            Name: "db1"
            PlanResolution: 50
            Coordinators: 1
            Mediators: 1
            TimeCastBucketsPerMediator: 2
            StoragePools {
                Name: "pool-1"
                Kind: "hdd"
            }
            ExternalSchemeShard: true
            ExternalHive: true
            GraphShard: true
        )");

        env.TestWaitNotification(runtime, txId);

        NKikimrScheme::TEvDescribeSchemeResult result = DescribePath(runtime, "/MyRoot/db1");
        UNIT_ASSERT(result.GetPathDescription().GetDomainDescription().GetProcessingParams().GetGraphShard() != 0);

        IActor* service = NGraph::CreateGraphService("/MyRoot/db1");
        TActorId serviceId = runtime.Register(service);
        runtime.RegisterService(NGraph::MakeGraphServiceId(), serviceId);
        TActorId sender = runtime.AllocateEdgeActor();

        // this call is needed to wait for establishing of pipe connection
        {
            NGraph::TEvGraph::TEvGetMetrics* event = new NGraph::TEvGraph::TEvGetMetrics();
            event->Record.AddMetrics("test.metric1");
            runtime.Send(NGraph::MakeGraphServiceId(), sender, event);
            TAutoPtr<IEventHandle> handle;
            NGraph::TEvGraph::TEvMetricsResult* response = runtime.GrabEdgeEventRethrow<NGraph::TEvGraph::TEvMetricsResult>(handle);
            Ctest << "Received result: " << response->Record.ShortDebugString() << Endl;
        }

        runtime.SimulateSleep(TDuration::Seconds(1));

        {
            NGraph::TEvGraph::TEvSendMetrics* event = new NGraph::TEvGraph::TEvSendMetrics();
            NKikimrGraph::TMetric* metric = event->Record.AddMetrics();
            metric->SetName("test.metric1");
            metric->SetValue(13);
            runtime.Send(NGraph::MakeGraphServiceId(), sender, event);
        }

        runtime.SimulateSleep(TDuration::Seconds(1));

        {
            NGraph::TEvGraph::TEvSendMetrics* event = new NGraph::TEvGraph::TEvSendMetrics();
            NKikimrGraph::TMetric* metric = event->Record.AddMetrics();
            metric->SetName("test.metric1");
            metric->SetValue(14);
            runtime.Send(NGraph::MakeGraphServiceId(), sender, event);
        }

        runtime.SimulateSleep(TDuration::Seconds(1));

        {
            NGraph::TEvGraph::TEvSendMetrics* event = new NGraph::TEvGraph::TEvSendMetrics();
            NKikimrGraph::TMetric* metric = event->Record.AddMetrics();
            metric->SetName("test.metric1");
            metric->SetValue(15);
            runtime.Send(NGraph::MakeGraphServiceId(), sender, event);
        }

        {
            NGraph::TEvGraph::TEvGetMetrics* event = new NGraph::TEvGraph::TEvGetMetrics();
            event->Record.AddMetrics("test.metric1");
            runtime.Send(NGraph::MakeGraphServiceId(), sender, event);
            TAutoPtr<IEventHandle> handle;
            NGraph::TEvGraph::TEvMetricsResult* response = runtime.GrabEdgeEventRethrow<NGraph::TEvGraph::TEvMetricsResult>(handle);
            Ctest << "Received result: " << response->Record.ShortDebugString() << Endl;
            UNIT_ASSERT(response->Record.DataSize() > 0);
            UNIT_ASSERT(response->Record.GetData(0).ShortDebugString() == "Values: 13 Values: 14");
        }
    }
}

} // NKikimr
