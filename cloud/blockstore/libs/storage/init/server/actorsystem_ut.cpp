#include "actorsystem.h"

#include <cloud/blockstore/libs/storage/partition/model/merged_blob_compression_policy.h>

#include <cloud/storage/core/libs/kikimr/actorsystem.h>

#include <contrib/ydb/core/base/domain.h>
#include <contrib/ydb/core/protos/config.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

Y_UNIT_TEST_SUITE(TServerActorSystemTest)
{
    Y_UNIT_TEST(ShouldRegisterMergedCompressionCountersThroughServerFactory)
    {
        TServerActorSystemArgs args{};
        args.NodeId = 1;
        args.AppConfig = std::make_shared<NKikimrConfig::TAppConfig>();
        args.AppConfig->MutableLogConfig();
        // TxProxy is constructed by the factory before actors are started.
        auto* domain = args.AppConfig->MutableDomainsConfig()->AddDomain();
        domain->SetDomainId(1);
        domain->SetName("Root");
        domain->AddExplicitAllocators(
            NKikimr::TDomainsInfo::MakeTxAllocatorIDFixed(1));
        auto* system = args.AppConfig->MutableActorSystemConfig();
        system->SetUseAutoConfig(true);
        system->SetCpuCount(2);
        auto* node = args.AppConfig->MutableNameserviceConfig()->AddNode();
        node->SetNodeId(args.NodeId);
        node->SetHost("localhost");
        node->SetAddress("::1");
        node->SetPort(0);
        args.StartupBlockstoreConfig = MakeBlockstoreConfig(
            {},
            {}, std::make_shared<TStorageConfigControls>());
        args.TemporaryServer = true;

        // Exercise the real server factory and its service initializers. No
        // actor threads or network listeners are started by this test.
        auto actorSystem = CreateActorSystem(args);
        UNIT_ASSERT(actorSystem);
        auto blockstore =
            actorSystem->GetCounters()->FindSubgroup("counters", "blockstore");
        UNIT_ASSERT(blockstore);
        auto compression =
            blockstore->FindSubgroup("component", "merged_blob_compression");
        UNIT_ASSERT(compression);
        auto rejections = compression->FindCounter("CompatibilityRejections");
        UNIT_ASSERT(rejections);
        const auto before = rejections->Val();
        NPartition::ReportMergedBlobCompatibilityRejection();
        UNIT_ASSERT_VALUES_EQUAL(rejections->Val(), before + 1);

        for (bool background: {false, true}) {
            auto scope = compression->FindSubgroup(
                "scope", background ? "background" : "foreground");
            UNIT_ASSERT(scope);
            for (bool read: {false, true}) {
                auto pool = scope->FindSubgroup(
                    "operation", read ? "decode" : "encode");
                UNIT_ASSERT(pool);
                const auto counter = [&](const TString& name)
                {
                    auto value = pool->FindCounter(name);
                    UNIT_ASSERT_C(value, name);
                    return value->Val();
                };
                const ui64 byteLimit = (background ? 256ULL : 512ULL) << 20;
                const ui32 slotLimit = background ? 2 : 4;
                UNIT_ASSERT_VALUES_EQUAL(
                    counter("ReservationLimitBytes"), byteLimit);
                UNIT_ASSERT_VALUES_EQUAL(
                    counter("ActiveOperationLimit"), slotLimit);
                UNIT_ASSERT_VALUES_EQUAL(counter("QueuedOperations"), 0);
                UNIT_ASSERT_VALUES_EQUAL(counter("ReservedBytes"), 0);
                UNIT_ASSERT_VALUES_EQUAL(counter("ActiveOperations"), 0);
                const auto attempts = counter("AdmissionAttempts");
                const auto rejected = counter("AdmissionRejected");
                auto token = NPartition::TryAcquireMergedBlobBudget(
                    background, read, byteLimit);
                UNIT_ASSERT(token);
                UNIT_ASSERT_VALUES_EQUAL(counter("ReservedBytes"), byteLimit);
                UNIT_ASSERT_VALUES_EQUAL(counter("ActiveOperations"), 1);
                UNIT_ASSERT(!NPartition::TryAcquireMergedBlobBudget(
                    background, read, 1));
                UNIT_ASSERT_VALUES_EQUAL(
                    counter("AdmissionAttempts"), attempts + 2);
                UNIT_ASSERT_VALUES_EQUAL(
                    counter("AdmissionRejected"), rejected + 1);
                token.reset();
                UNIT_ASSERT_VALUES_EQUAL(counter("ReservedBytes"), 0);
                UNIT_ASSERT_VALUES_EQUAL(counter("ActiveOperations"), 0);
            }
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
