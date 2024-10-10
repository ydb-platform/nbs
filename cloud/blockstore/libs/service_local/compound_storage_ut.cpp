#include "compound_storage.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/server/config.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/iterator_range.h>

namespace NCloud::NBlockStore::NServer {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 DefaultBlockSize = 8;

////////////////////////////////////////////////////////////////////////////////

struct TTestStorage final
    : public IStorage
{
    TString Data;

    explicit TTestStorage(ui32 blockCount, char fill = 0)
        : Data(blockCount * DefaultBlockSize, fill)
    {}

    ui32 BlockCount()
    {
        return Data.size() / DefaultBlockSize;
    }

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        Y_UNUSED(callContext);

        const auto offset = request->GetStartIndex() * DefaultBlockSize;
        const auto bytes = request->GetBlocksCount() * DefaultBlockSize;

        std::fill_n(Data.begin() + offset, bytes, '\0');

        return MakeFuture(NProto::TZeroBlocksResponse());
    }

    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override
    {
        Y_UNUSED(callContext);

        auto guard = request->Sglist.Acquire();
        UNIT_ASSERT(guard);

        const auto& dst = guard.Get();

        const auto offset = request->GetStartIndex() * DefaultBlockSize;
        const auto bytes = SgListGetSize(dst);

        SgListCopy({ Data.begin() + offset, bytes }, dst);

        return MakeFuture(NProto::TReadBlocksLocalResponse());
    }

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        Y_UNUSED(callContext);

        auto guard = request->Sglist.Acquire();
        UNIT_ASSERT(guard);

        const auto& src = guard.Get();

        const auto offset = request->GetStartIndex() * DefaultBlockSize;
        const auto bytes = SgListGetSize(src);

        SgListCopy(src, { Data.begin() + offset, bytes });

        return MakeFuture(NProto::TWriteBlocksLocalResponse());
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        size_t space = bytesCount + DefaultBlockSize;

        void* p = std::malloc(space);

        Y_ABORT_UNLESS(std::align(DefaultBlockSize, bytesCount, p, space));

        return { static_cast<char*>(p), &std::free };
    }

    TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method) override
    {
        Y_UNUSED(method);

        std::fill_n(Data.begin(), Data.size(), '\0');

        return MakeFuture(NProto::TError());
    }

    void ReportIOError() override
    {}
};

template <typename R>
void SetBlocks(R& request, ui32 blockCount, char fill = 0)
{
    auto& buffers = *request.MutableBlocks()->MutableBuffers();
    buffers.Reserve(blockCount);

    for (ui32 i = 0; i != blockCount; ++i) {
        buffers.Add()->resize(DefaultBlockSize, fill);
    }
}

TSgList CreateSgListFromBuffer(TString& buffer, ui64 startIndex, ui64 blockCount)
{
    UNIT_ASSERT(startIndex * DefaultBlockSize <= buffer.size());
    UNIT_ASSERT(blockCount * DefaultBlockSize <= buffer.size());

    TSgList sglist(blockCount);
    auto data = &buffer[0] + startIndex * DefaultBlockSize;

    for (auto& buf: sglist) {
        buf = { data, DefaultBlockSize };
        data += DefaultBlockSize;
    }

    return sglist;
}

TSgList CreateSgListFromBuffer(TString& buffer)
{
    UNIT_ASSERT(buffer.size() % DefaultBlockSize == 0);

    return CreateSgListFromBuffer(buffer, 0, buffer.size() / DefaultBlockSize);
}

IStoragePtr CreateTestStorage(
    TVector<std::shared_ptr<TTestStorage>> storages,
    IMonitoringServicePtr monitoring)
{
    TVector<ui64> offsets(Reserve(storages.size()));

    ui64 offset = 0;
    for (const auto& storage: storages) {
        offset += storage->BlockCount();
        offsets.push_back(offset);
    }

    auto serverGroup = monitoring->GetCounters()
        ->GetSubgroup("counters", "blockstore")
        ->GetSubgroup("component", "server");

    auto serverStats = CreateServerStats(
        std::make_shared<TServerAppConfig>(),
        std::make_shared<TDiagnosticsConfig>(),
        monitoring,
        CreateProfileLogStub(),
        CreateServerRequestStats(
            serverGroup,
            CreateWallClockTimer(),
            EHistogramCounterOption::ReportMultipleCounters),
        CreateVolumeStatsStub());

    return CreateCompoundStorage(
        { storages.begin(), storages.end() },
        std::move(offsets),
        DefaultBlockSize,
        {}, // diskId
        {}, // clientId
        std::move(serverStats));
}

auto RequestReadBlocksLocal(IStoragePtr storage, ui64 startIndex, ui32 blockCount)
{
    auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
    request->SetStartIndex(startIndex);
    request->SetBlocksCount(blockCount);
    request->BlockSize = DefaultBlockSize;

    const auto bytes = blockCount * DefaultBlockSize;
    auto buffer = storage->AllocateBuffer(bytes);

    if (bytes) {
        request->Sglist.SetSgList({{ buffer.get(), bytes }});
    }

    return storage->ReadBlocksLocal(
        MakeIntrusive<TCallContext>(),
        std::move(request)).ExtractValueSync();
}

TString ReadBlocksLocal(IStoragePtr storage, ui64 startIndex, ui32 blockCount)
{
    auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
    request->SetStartIndex(startIndex);
    request->SetBlocksCount(blockCount);
    request->BlockSize = DefaultBlockSize;

    const auto bytes = blockCount * DefaultBlockSize;
    auto buffer = storage->AllocateBuffer(bytes);

    if (bytes) {
        request->Sglist.SetSgList({{ buffer.get(), bytes }});
    }

    auto response = storage->ReadBlocksLocal(
        MakeIntrusive<TCallContext>(),
        std::move(request)).ExtractValueSync();

    UNIT_ASSERT(!HasError(response));

    return TString(buffer.get(), bytes);
}

void ValidateBlocks(IStoragePtr storage, ui64 startIndex, ui32 blockCount, char value)
{
    const TString expected(DefaultBlockSize, value);
    TString buffer = ReadBlocksLocal(storage, startIndex, blockCount);

    for (auto s: CreateSgListFromBuffer(buffer)) {
        UNIT_ASSERT_VALUES_EQUAL(expected, s.AsStringBuf());
    }
}

auto RequestWriteBlocksLocal(
    IStoragePtr storage,
    ui64 startIndex,
    ui32 blockCount,
    char value)
{
    auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
    request->SetStartIndex(startIndex);
    request->BlocksCount = blockCount;
    request->BlockSize = DefaultBlockSize;

    const auto bytes = blockCount * DefaultBlockSize;

    auto buffer = storage->AllocateBuffer(bytes);
    memset(buffer.get(), value, bytes);

    if (bytes) {
        request->Sglist.SetSgList({{ buffer.get(), bytes }});
    }

    return storage->WriteBlocksLocal(
        MakeIntrusive<TCallContext>(),
        std::move(request)).ExtractValueSync();
}

void WriteBlocksLocal(IStoragePtr storage, ui64 startIndex, ui32 blockCount, char value)
{
    auto response = RequestWriteBlocksLocal(storage, startIndex, blockCount, value);
    UNIT_ASSERT(!HasError(response));
}

auto RequestZeroBlocks(IStoragePtr storage, ui64 startIndex, ui32 blockCount)
{
    auto request = std::make_shared<NProto::TZeroBlocksRequest>();
    request->SetStartIndex(startIndex);
    request->SetBlocksCount(blockCount);

    return storage->ZeroBlocks(
        MakeIntrusive<TCallContext>(),
        std::move(request)).ExtractValueSync();
}

void ZeroBlocks(IStoragePtr storage, ui64 startIndex, ui32 blockCount)
{
    auto response = RequestZeroBlocks(storage, startIndex, blockCount);

    UNIT_ASSERT(!HasError(response));
}

auto GetCounter(IMonitoringServicePtr monitoring, const TString& request)
{
    return monitoring->GetCounters()
        ->GetSubgroup("counters", "blockstore")
        ->GetSubgroup("component", "server")
        ->GetSubgroup("request", request)
        ->GetCounter("FastPathHits");
}

ui64 GetReadFastPathCounterValue(IMonitoringServicePtr monitoring)
{
    return GetCounter(monitoring, "ReadBlocks")->Val();
}

ui64 GetWriteFastPathCounterValue(IMonitoringServicePtr monitoring)
{
    return GetCounter(monitoring, "WriteBlocks")->Val();
}

ui64 GetZeroFastPathCounterValue(IMonitoringServicePtr monitoring)
{
    return GetCounter(monitoring, "ZeroBlocks")->Val();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCompoundStorageTest)
{
    Y_UNIT_TEST(ShouldZero)
    {
        auto monitoring = CreateMonitoringServiceStub();

        auto storage = CreateTestStorage(
            {
                std::make_shared<TTestStorage>(100, 'A'),
                std::make_shared<TTestStorage>(200, 'B'),
                std::make_shared<TTestStorage>(300, 'C')
            },
            monitoring);

        UNIT_ASSERT_VALUES_EQUAL(0, GetReadFastPathCounterValue(monitoring));
        UNIT_ASSERT_VALUES_EQUAL(0, GetWriteFastPathCounterValue(monitoring));
        UNIT_ASSERT_VALUES_EQUAL(0, GetZeroFastPathCounterValue(monitoring));

        ZeroBlocks(storage, 50, 150);
        UNIT_ASSERT_VALUES_EQUAL(0, GetWriteFastPathCounterValue(monitoring));

        ValidateBlocks(storage, 0, 50, 'A');
        ValidateBlocks(storage, 50, 150, '\0');
        ValidateBlocks(storage, 200, 100, 'B');
        ValidateBlocks(storage, 300, 300, 'C');

        ZeroBlocks(storage, 0, 1);
        UNIT_ASSERT_VALUES_EQUAL(1, GetZeroFastPathCounterValue(monitoring));

        ValidateBlocks(storage, 0, 1, '\0');
        ValidateBlocks(storage, 1, 49, 'A');
        ValidateBlocks(storage, 50, 150, '\0');
        ValidateBlocks(storage, 200, 100, 'B');
        ValidateBlocks(storage, 300, 300, 'C');

        ZeroBlocks(storage, 0, 200);
        UNIT_ASSERT_VALUES_EQUAL(1, GetZeroFastPathCounterValue(monitoring));

        ValidateBlocks(storage, 0, 200, '\0');
        ValidateBlocks(storage, 200, 100, 'B');
        ValidateBlocks(storage, 300, 300, 'C');

        ZeroBlocks(storage, 0, 600);
        UNIT_ASSERT_VALUES_EQUAL(1, GetZeroFastPathCounterValue(monitoring));

        ValidateBlocks(storage, 0, 600, '\0');
    }

    Y_UNIT_TEST(ShouldReadLocal)
    {
        auto monitoring = CreateMonitoringServiceStub();

        auto storage = CreateTestStorage(
            {
                std::make_shared<TTestStorage>(100, 'A'),
                std::make_shared<TTestStorage>(200, 'B'),
                std::make_shared<TTestStorage>(300, 'C')
            },
            monitoring);

        UNIT_ASSERT_VALUES_EQUAL(0, GetReadFastPathCounterValue(monitoring));

        {
            auto buffer = ReadBlocksLocal(storage, 0, 100);
            for (auto buf: CreateSgListFromBuffer(buffer)) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'A'), buf.AsStringBuf());
            }
        }

        {
            auto buffer = ReadBlocksLocal(storage, 100, 200);
            for (auto buf: CreateSgListFromBuffer(buffer)) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'B'), buf.AsStringBuf());
            }
        }

        {
            auto buffer = ReadBlocksLocal(storage, 300, 300);
            for (auto buf: CreateSgListFromBuffer(buffer)) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'C'), buf.AsStringBuf());
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(3, GetReadFastPathCounterValue(monitoring));

        {
            auto buffer = ReadBlocksLocal(storage, 0, 600);
            for (auto buf: CreateSgListFromBuffer(buffer, 0, 100)) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'A'), buf.AsStringBuf());
            }

            for (auto buf: CreateSgListFromBuffer(buffer, 100, 200)) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'B'), buf.AsStringBuf());
            }

            for (auto buf: CreateSgListFromBuffer(buffer, 300, 300)) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'C'), buf.AsStringBuf());
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(3, GetReadFastPathCounterValue(monitoring));

        {
            auto buffer = ReadBlocksLocal(storage, 50, 150);
            for (auto buf: CreateSgListFromBuffer(buffer, 0, 50)) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'A'), buf.AsStringBuf());
            }

            for (auto buf: CreateSgListFromBuffer(buffer, 50, 100)) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'B'), buf.AsStringBuf());
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(3, GetReadFastPathCounterValue(monitoring));
        {
            auto buffer = ReadBlocksLocal(storage, 550, 10);
            for (auto buf: CreateSgListFromBuffer(buffer)) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'C'), buf.AsStringBuf());
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(4, GetReadFastPathCounterValue(monitoring));
    }

    Y_UNIT_TEST(ShouldWriteLocal)
    {
         auto monitoring = CreateMonitoringServiceStub();

        auto storage = CreateTestStorage(
            {
                std::make_shared<TTestStorage>(100, 'A'),
                std::make_shared<TTestStorage>(200, 'B'),
                std::make_shared<TTestStorage>(300, 'C')
            },
            monitoring);

        UNIT_ASSERT_VALUES_EQUAL(0, GetReadFastPathCounterValue(monitoring));
        UNIT_ASSERT_VALUES_EQUAL(0, GetWriteFastPathCounterValue(monitoring));

        WriteBlocksLocal(storage, 50, 150, 'X');

        ValidateBlocks(storage, 0, 50, 'A');
        ValidateBlocks(storage, 50, 150, 'X');
        ValidateBlocks(storage, 200, 100, 'B');
        ValidateBlocks(storage, 300, 300, 'C');

        WriteBlocksLocal(storage, 0, 50, 'X');
        UNIT_ASSERT_VALUES_EQUAL(1, GetWriteFastPathCounterValue(monitoring));

        ValidateBlocks(storage, 0, 200, 'X');
        ValidateBlocks(storage, 200, 100, 'B');
        ValidateBlocks(storage, 300, 300, 'C');

        WriteBlocksLocal(storage, 200, 100, 'Y');
        UNIT_ASSERT_VALUES_EQUAL(2, GetWriteFastPathCounterValue(monitoring));

        ValidateBlocks(storage, 0, 200, 'X');
        ValidateBlocks(storage, 200, 100, 'Y');
        ValidateBlocks(storage, 300, 300, 'C');

        WriteBlocksLocal(storage, 300, 300, 'Z');
        UNIT_ASSERT_VALUES_EQUAL(3, GetWriteFastPathCounterValue(monitoring));

        ValidateBlocks(storage, 0, 200, 'X');
        ValidateBlocks(storage, 200, 100, 'Y');
        ValidateBlocks(storage, 300, 300, 'Z');
    }

    Y_UNIT_TEST(ShouldHandleEmptyRanges)
    {
        auto monitoring = CreateMonitoringServiceStub();

        auto storage = CreateTestStorage(
            {
                std::make_shared<TTestStorage>(100, 'X'),
                std::make_shared<TTestStorage>(200, 'X'),
                std::make_shared<TTestStorage>(300, 'X')
            },
            monitoring);

        UNIT_ASSERT(ReadBlocksLocal(storage, 0, 0).empty());
        UNIT_ASSERT(ReadBlocksLocal(storage, 100, 0).empty());
        UNIT_ASSERT(ReadBlocksLocal(storage, 600, 0).empty());

        WriteBlocksLocal(storage, 0, 0, 'A');
        WriteBlocksLocal(storage, 100, 0, 'B');
        WriteBlocksLocal(storage, 600, 0, 'C');

        ZeroBlocks(storage, 0, 0);
        ZeroBlocks(storage, 50, 0);
        ZeroBlocks(storage, 300, 0);

        ValidateBlocks(storage, 0, 600, 'X');

        UNIT_ASSERT_VALUES_EQUAL(0, GetReadFastPathCounterValue(monitoring));
        UNIT_ASSERT_VALUES_EQUAL(0, GetWriteFastPathCounterValue(monitoring));
        UNIT_ASSERT_VALUES_EQUAL(0, GetZeroFastPathCounterValue(monitoring));
    }

    Y_UNIT_TEST(ShouldHandleInvalidRanges)
    {
        auto monitoring = CreateMonitoringServiceStub();

        auto storage = CreateTestStorage(
            {
                std::make_shared<TTestStorage>(100, 'A'),
                std::make_shared<TTestStorage>(200, 'B'),
                std::make_shared<TTestStorage>(300, 'C')
            },
            monitoring);

        // reads local

        {
            auto response = RequestReadBlocksLocal(storage, 1000, 1);
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response.GetError().GetCode());
        }

        {
            auto response = RequestReadBlocksLocal(storage, 0, 1000);
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response.GetError().GetCode());
        }

        {
            auto response = RequestReadBlocksLocal(storage, 599, 2);
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response.GetError().GetCode());
        }

        {
            auto response = RequestReadBlocksLocal(storage, 600, 1);
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response.GetError().GetCode());
        }

        {
            auto response = RequestReadBlocksLocal(storage, 0, 601);
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response.GetError().GetCode());
        }

        // writes local

        {
            auto response = RequestWriteBlocksLocal(storage, 1000, 1, 'X');
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response.GetError().GetCode());
        }

        {
            auto response = RequestWriteBlocksLocal(storage, 0, 1000, 'X');
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response.GetError().GetCode());
        }

        {
            auto response = RequestWriteBlocksLocal(storage, 599, 2, 'X');
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response.GetError().GetCode());
        }

        {
            auto response = RequestWriteBlocksLocal(storage, 600, 1, 'X');
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response.GetError().GetCode());
        }

        {
            auto response = RequestWriteBlocksLocal(storage, 0, 601, 'X');
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response.GetError().GetCode());
        }

        // zeroes

        {
            auto response = RequestZeroBlocks(storage, 1000, 1);
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response.GetError().GetCode());
        }

        {
            auto response = RequestZeroBlocks(storage, 0, 1000);
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response.GetError().GetCode());
        }

        {
            auto response = RequestZeroBlocks(storage, 599, 2);
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response.GetError().GetCode());
        }

        {
            auto response = RequestZeroBlocks(storage, 600, 1);
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response.GetError().GetCode());
        }

        {
            auto response = RequestZeroBlocks(storage, 0, 601);
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response.GetError().GetCode());
        }

        UNIT_ASSERT_VALUES_EQUAL(0, GetReadFastPathCounterValue(monitoring));
        UNIT_ASSERT_VALUES_EQUAL(0, GetWriteFastPathCounterValue(monitoring));
        UNIT_ASSERT_VALUES_EQUAL(0, GetZeroFastPathCounterValue(monitoring));
    }

    Y_UNIT_TEST(ShouldHandleSingleStorage)
    {
        auto monitoring = CreateMonitoringServiceStub();

        auto storage = CreateTestStorage(
            {
                std::make_shared<TTestStorage>(500, 'A'),
            },
            monitoring);

        UNIT_ASSERT_VALUES_EQUAL(0, GetReadFastPathCounterValue(monitoring));
        UNIT_ASSERT_VALUES_EQUAL(0, GetWriteFastPathCounterValue(monitoring));
        UNIT_ASSERT_VALUES_EQUAL(0, GetZeroFastPathCounterValue(monitoring));

        ValidateBlocks(storage, 0, 100, 'A');
        UNIT_ASSERT_VALUES_EQUAL(1, GetReadFastPathCounterValue(monitoring));

        ValidateBlocks(storage, 100, 400, 'A');
        UNIT_ASSERT_VALUES_EQUAL(2, GetReadFastPathCounterValue(monitoring));

        WriteBlocksLocal(storage, 100, 100, 'B');
        ValidateBlocks(storage, 100, 100, 'B');

        UNIT_ASSERT_VALUES_EQUAL(1, GetWriteFastPathCounterValue(monitoring));
        UNIT_ASSERT_VALUES_EQUAL(3, GetReadFastPathCounterValue(monitoring));

        WriteBlocksLocal(storage, 100, 100, 'B');
        ValidateBlocks(storage, 0, 100, 'A');
        ValidateBlocks(storage, 100, 100, 'B');
        ValidateBlocks(storage, 200, 300, 'A');
        ValidateBlocks(storage, 200, 300, 'A');

        UNIT_ASSERT_VALUES_EQUAL(2, GetWriteFastPathCounterValue(monitoring));
        UNIT_ASSERT_VALUES_EQUAL(7, GetReadFastPathCounterValue(monitoring));
        UNIT_ASSERT_VALUES_EQUAL(0, GetZeroFastPathCounterValue(monitoring));

        ZeroBlocks(storage, 0, 200);
        ValidateBlocks(storage, 0, 200, '\0');
        ValidateBlocks(storage, 0, 200, '\0');
        ValidateBlocks(storage, 200, 300, 'A');

        UNIT_ASSERT_VALUES_EQUAL(2, GetWriteFastPathCounterValue(monitoring));
        UNIT_ASSERT_VALUES_EQUAL(10, GetReadFastPathCounterValue(monitoring));
        UNIT_ASSERT_VALUES_EQUAL(1, GetZeroFastPathCounterValue(monitoring));

        WriteBlocksLocal(storage, 100, 200, 'C');
        ValidateBlocks(storage, 100, 200, 'C');
    }

    Y_UNIT_TEST(ShouldHandleRecursiveStorage)
    {
        auto monitoring = CreateMonitoringServiceStub();

        auto storage1 = CreateTestStorage(
            {
                std::make_shared<TTestStorage>(100, 'A'),
                std::make_shared<TTestStorage>(100, 'B'),
                std::make_shared<TTestStorage>(100, 'C')
            },
            monitoring);

        auto storage2 = CreateTestStorage(
            {
                std::make_shared<TTestStorage>(200, 'X'),
                std::make_shared<TTestStorage>(200, 'Y'),
                std::make_shared<TTestStorage>(200, 'Z')
            },
            monitoring);

        auto storage3 = CreateTestStorage(
            {
                std::make_shared<TTestStorage>(300, '1'),
                std::make_shared<TTestStorage>(300, '2'),
                std::make_shared<TTestStorage>(300, '3')
            },
            monitoring);

        auto serverGroup = monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "server");

        auto serverStats = CreateServerStats(
            std::make_shared<TServerAppConfig>(),
            std::make_shared<TDiagnosticsConfig>(),
            monitoring,
            CreateProfileLogStub(),
            CreateServerRequestStats(
                serverGroup,
                CreateWallClockTimer(),
                EHistogramCounterOption::ReportMultipleCounters),
            CreateVolumeStatsStub());

        auto storage = CreateCompoundStorage(
            { storage1, storage2, storage3 },
            { 300, 900, 1800 },
            DefaultBlockSize,
            {}, // diskId
            {}, // clientId
            std::move(serverStats));

        ValidateBlocks(storage,   0, 100, 'A');
        ValidateBlocks(storage, 100, 100, 'B');
        ValidateBlocks(storage, 200, 100, 'C');

        ValidateBlocks(storage, 300, 200, 'X');
        ValidateBlocks(storage, 500, 200, 'Y');
        ValidateBlocks(storage, 700, 200, 'Z');

        ValidateBlocks(storage,  900, 300, '1');
        ValidateBlocks(storage, 1200, 300, '2');
        ValidateBlocks(storage, 1500, 300, '3');

        WriteBlocksLocal(storage,   0, 300, 'K');
        WriteBlocksLocal(storage, 300, 600, 'L');
        WriteBlocksLocal(storage, 900, 900, 'M');

        ValidateBlocks(storage1,   0, 300, 'K');
        ValidateBlocks(storage2,   0, 600, 'L');
        ValidateBlocks(storage3,   0, 900, 'M');

        ValidateBlocks(storage,   0, 300, 'K');
        ValidateBlocks(storage, 300, 600, 'L');
        ValidateBlocks(storage, 900, 900, 'M');

        ZeroBlocks(storage, 0, 1800);

        ValidateBlocks(storage1,   0, 300, '\0');
        ValidateBlocks(storage2,   0, 600, '\0');
        ValidateBlocks(storage3,   0, 900, '\0');

        ValidateBlocks(storage,   0, 300, '\0');
        ValidateBlocks(storage, 300, 600, '\0');
        ValidateBlocks(storage, 900, 900, '\0');

        WriteBlocksLocal(storage, 0, 600, '-');
        ValidateBlocks(storage, 0, 600, '-');

        WriteBlocksLocal(storage, 600, 1200, '+');
        ValidateBlocks(storage, 600, 1200, '+');

        WriteBlocksLocal(storage, 1200, 600, '=');
        ValidateBlocks(storage, 1200, 600, '=');

        {
            auto buffer = ReadBlocksLocal(storage, 0, 1800);
            for (auto buf: CreateSgListFromBuffer(buffer, 0, 600)) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, '-'), buf.AsStringBuf());
            }

            for (auto buf: CreateSgListFromBuffer(buffer, 600, 600)) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, '+'), buf.AsStringBuf());
            }

            for (auto buf: CreateSgListFromBuffer(buffer, 1200, 600)) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, '='), buf.AsStringBuf());
            }
        }
    }

    Y_UNIT_TEST(ShouldHandleCancelLocalRequest)
    {
        auto monitoring = CreateMonitoringServiceStub();

        auto storage = CreateTestStorage(
            {
                std::make_shared<TTestStorage>(100, 'A'),
                std::make_shared<TTestStorage>(200, 'B'),
                std::make_shared<TTestStorage>(300, 'C')
            },
            monitoring);

        {
            auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
            request->SetStartIndex(0);
            request->BlocksCount = 100;
            request->BlockSize = DefaultBlockSize;
            request->Sglist.Close();

            auto response = storage->WriteBlocksLocal(
                MakeIntrusive<TCallContext>(),
                std::move(request)).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(E_CANCELLED, response.GetError().GetCode());
        }

        {
            auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
            request->SetBlocksCount(100);
            request->BlockSize = DefaultBlockSize;
            request->Sglist.Close();

            auto response = storage->ReadBlocksLocal(
                MakeIntrusive<TCallContext>(),
                std::move(request)).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(E_CANCELLED, response.GetError().GetCode());
        }
    }
}

}   // namespace NCloud::NBlockStore::NServer
