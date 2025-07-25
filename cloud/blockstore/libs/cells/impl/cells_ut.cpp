#include "cells.h"

#include <cloud/blockstore/config/cells.pb.h>
#include <cloud/blockstore/config/client.pb.h>
#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/guid.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NCells {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestBlockStore
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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCellsManagerTest)
{
    Y_UNIT_TEST(ShouldNotReturnDescribeFutureIfNoCellsConfigures)
    {
        TArguments args;

        auto cells = std::make_shared<TCellsManager>(
            std::make_shared<TCellsConfig>(),
            args);

        auto optionalFuture = cells->DescribeVolume(
            "disk",
            {},
            std::make_shared<TTestBlockStore>(),
            {}
        );

        UNIT_ASSERT_C(
            !optionalFuture.has_value(),
            "future should not be returned");
    }
}

}   // namespace NCloud::NBlockStore::NCells
