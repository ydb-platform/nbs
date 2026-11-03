#include "part_ut.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/testlib/part_client.h>

#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/core/base/logoblob.h>
#include <contrib/ydb/core/base/tablet.h>

#include <library/cpp/testing/unittest/registar.h>

#include <memory>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;
using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPartitionDynconfigTest)
{
    // Verify propagation of dynamic configuration updates from the partition to
    // its existing TIOCompanion without resetting the error history.
    // Use MaxReadBlobErrorsBeforeSuicide to observe the applied configuration.
    Y_UNIT_TEST(ShouldApplyDynamicConfigUpdateToIOCompanion)
    {
        // Persist a data blob before injecting errors into its reads.
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1_MB);
        config.SetMaxReadBlobErrorsBeforeSuicide(5);
        auto r = PrepareTestActorRuntime(config);
        auto& runtime = *r;
        TPartitionClient partition(runtime);
        partition.WaitReady();
        const auto range = TBlockRange32::WithLength(0, 1001);
        partition.WriteBlocks(range, 1);
        const auto description = partition.DescribeBlocks(range, "");
        UNIT_ASSERT_VALUES_EQUAL(1, description->Record.BlobPiecesSize());
        const auto blobId = LogoBlobIDFromLogoBlobID(
            description->Record.GetBlobPieces(0).GetBlobId());

        // Fail only reads of that blob and observe the tablet's termination.
        bool died = false;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() == TEvTablet::EEv::EvTabletDead) {
                    died = true;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });
        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() !=
                    TEvBlobStorage::TEvGet::EventType)
                {
                    return false;
                }
                const auto* request = event->Get<TEvBlobStorage::TEvGet>();

                // Fail the whole batch if any query targets the
                // data blob selected by the test.
                for (ui32 i = 0; i != request->QuerySize; ++i) {
                    if (blobId.IsSameBlob(request->Queries[i].Id)) {
                        // Emulate a BlobStorage reply to the requester.
                        // Preserve the cookie to match the original request.
                        runtime.Schedule(
                            new IEventHandle(
                                event->Sender,
                                event->Recipient,
                                new TEvBlobStorage::TEvGetResult(
                                    NKikimrProto::ERROR, request->QuerySize, 0),
                                0,
                                event->Cookie),
                            TDuration());

                        // Drop the request to prevent a real reply
                        // from BlobStorage.
                        return true;
                    }
                }
                return false;
            });
        partition.SendReadBlocksRequest(0);
        UNIT_ASSERT(FAILED(partition.RecvReadBlocksResponse()->GetStatus()));
        UNIT_ASSERT(!died);

        // Lower the threshold while retaining the first recorded error.
        config.SetMaxReadBlobErrorsBeforeSuicide(2);
        partition.SendToPipe(
            std::make_unique<TEvVolume::TEvDynamicConfigUpdate>(
                std::make_shared<TStorageConfig>(
                    config, std::make_shared<NFeatures::TFeaturesConfig>()),
                TActorId(0, "volume"),
                1));
        partition.SendReadBlocksRequest(0);
        UNIT_ASSERT(FAILED(partition.RecvReadBlocksResponse()->GetStatus()));
        UNIT_ASSERT(died);
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
