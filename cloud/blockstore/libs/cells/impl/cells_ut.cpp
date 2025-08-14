#include "cells.h"

#include <cloud/blockstore/config/cells.pb.h>
#include <cloud/blockstore/config/client.pb.h>
#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/trace_serializer.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/guid.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NCells {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestBlockStoreBase
    : public IBlockStore
{
    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

    void Start() override
    {}

    void Stop() override
    {}

#define BLOCKSTORE_DECLARE_METHOD(name, ...)                                   \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        Y_UNUSED(callContext);                                                 \
        Y_UNUSED(request);                                                     \
        return MakeFuture<NProto::T##name##Response>();                        \
    }                                                                          \
// BLOCKSTORE_DECLARE_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD
};

////////////////////////////////////////////////////////////////////////////////

struct TTestBlockStore: TTestBlockStoreBase
{
    THashSet<TString> Disks;

    TFuture<NProto::TDescribeVolumeResponse> DescribeVolume(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TDescribeVolumeRequest> request) final
    {
        Y_UNUSED(callContext);
        NProto::TDescribeVolumeResponse response;
        if (!Disks.contains(request->GetDiskId())) {
            *response.MutableError() = MakeError(E_NOT_FOUND);
        }

        return MakeFuture(response);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCellManagerTest)
{
    Y_UNIT_TEST(ShouldNotReturnDescribeFutureIfNoCellsConfigures)
    {
        auto cells = CreateCellManager(
            std::make_shared<TCellsConfig>(),
            CreateWallClockTimer(),
            CreateSchedulerStub(),
            CreateLoggingService("console"),
            CreateMonitoringServiceStub(),
            CreateTraceSerializerStub(),
            CreateServerStatsStub(),
            nullptr   // rdmaClient
        );

        auto blockStore = std::make_shared<TTestBlockStore>();
        blockStore->Disks.insert("vol0");

        {
            auto future = cells->DescribeVolume(
                MakeIntrusive<TCallContext>(),
                "vol0",
                {},   // headers
                blockStore,
                {}   // clientConfig
            );

            auto response = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response.GetError().GetCode());
        }

        {
            auto future = cells->DescribeVolume(
                MakeIntrusive<TCallContext>(),
                "unknown",
                {},   // headers
                blockStore,
                {}   // clientConfig
            );

            auto response = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(
                E_NOT_FOUND,
                response.GetError().GetCode());
        }
    }
}

}   // namespace NCloud::NBlockStore::NCells
