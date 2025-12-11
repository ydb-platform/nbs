#include "storage_null.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/blockstore/libs/service/storage_provider.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/sglist.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>

namespace NCloud::NBlockStore::NServer {

// using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration WaitTimeout = TDuration::Seconds(5);
constexpr ui32 DefaultBlockSize = 4096;

////////////////////////////////////////////////////////////////////////////////

#define UNIT_ASSERT_SUCCEEDED(e) \
    UNIT_ASSERT_C(SUCCEEDED(e.GetCode()), e.GetMessage())

////////////////////////////////////////////////////////////////////////////////

auto ReadBlocksLocal(IStorage& storage, TGuardedSgList sglist)
{
    auto context = MakeIntrusive<TCallContext>();

    auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
    request->SetStartIndex(0);
    request->SetBlocksCount(1);
    request->BlockSize = DefaultBlockSize;
    request->Sglist = std::move(sglist);

    return storage.ReadBlocksLocal(std::move(context), std::move(request))
        .GetValue(WaitTimeout);
}

auto WriteBlocksLocal(IStorage& storage, TGuardedSgList sglist)
{
    auto context = MakeIntrusive<TCallContext>();

    auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
    request->SetStartIndex(0);
    request->BlocksCount = 1;
    request->BlockSize = DefaultBlockSize;
    request->Sglist = std::move(sglist);

    return storage.WriteBlocksLocal(std::move(context), std::move(request))
        .GetValue(WaitTimeout);
}

auto ZeroBlocks(IStorage& storage)
{
    auto context = MakeIntrusive<TCallContext>();

    auto request = std::make_shared<NProto::TZeroBlocksRequest>();
    request->SetStartIndex(0);
    request->SetBlocksCount(1);

    return storage.ZeroBlocks(std::move(context), std::move(request))
        .GetValue(WaitTimeout);
}

auto CreateStorage()
{
    auto provider = CreateNullStorageProvider();

    return provider
        ->CreateStorage(NProto::TVolume(), "", NProto::VOLUME_ACCESS_READ_WRITE)
        .GetValue();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TNullStorageTest)
{
    Y_UNIT_TEST(ShouldLocalReadWriteZero)
    {
        auto storage = CreateStorage();

        auto writeBuffer = TGuardedBuffer(TString(DefaultBlockSize, 'a'));
        auto writeSglist = writeBuffer.GetGuardedSgList();

        auto writeResponse = WriteBlocksLocal(*storage, std::move(writeSglist));
        UNIT_ASSERT_SUCCEEDED(writeResponse.GetError());

        auto readBuffer =
            TGuardedBuffer(TString::Uninitialized(DefaultBlockSize));
        auto readSglist = readBuffer.GetGuardedSgList();

        auto readResponse = ReadBlocksLocal(*storage, std::move(readSglist));
        UNIT_ASSERT_SUCCEEDED(readResponse.GetError());

        auto zeroResponse = ZeroBlocks(*storage);
        UNIT_ASSERT_SUCCEEDED(writeResponse.GetError());
    }
}

}   // namespace NCloud::NBlockStore::NServer
