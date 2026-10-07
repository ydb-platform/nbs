#include "compound_storage.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/server/config.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/device_handler.h>
#include <cloud/blockstore/libs/service/storage.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/io_depth_tracker.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/iterator_range.h>

#include <array>
#include <functional>
#include <type_traits>

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
    std::function<TFuture<NProto::TReadBlocksLocalResponse>(
        std::shared_ptr<NProto::TReadBlocksLocalRequest>)>
        ReadHandler;
    std::function<TFuture<NProto::TWriteBlocksLocalResponse>(
        std::shared_ptr<NProto::TWriteBlocksLocalRequest>)>
        WriteHandler;
    std::function<TFuture<NProto::TZeroBlocksResponse>(
        std::shared_ptr<NProto::TZeroBlocksRequest>)>
        ZeroHandler;
    std::function<TFuture<NProto::TError>()> EraseHandler;

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

        if (ZeroHandler) {
            return ZeroHandler(std::move(request));
        }

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

        if (ReadHandler) {
            return ReadHandler(std::move(request));
        }

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

        if (WriteHandler) {
            return WriteHandler(std::move(request));
        }

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

        if (EraseHandler) {
            return EraseHandler();
        }

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
            EHistogramCounterOption::ReportMultipleCounters,
            {}),
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
    request->SetBlockSize(DefaultBlockSize);

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
    request->SetBlockSize(DefaultBlockSize);

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
    request->SetBlockSize(DefaultBlockSize);

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

enum class ECompoundOperation
{
    Read,
    Write,
    Zero,
    Erase,
};

enum class EChildFailure
{
    Success,
    Error,
    Exception,
    SynchronousException,
};

template <typename TResponse>
void CompleteChild(TPromise<TResponse>& promise, const NProto::TError& error,
                   bool exceptional = false)
{
    if (exceptional) {
        promise.SetException(std::make_exception_ptr(
            TServiceError(error.GetCode()) << error.GetMessage()));
    } else if constexpr (std::is_same_v<TResponse, NProto::TError>) {
        promise.SetValue(error);
    } else {
        TResponse response;
        *response.MutableError() = error;
        promise.SetValue(std::move(response));
    }
}

template <typename TResponse>
NProto::TError GetChildError(const TPromise<TResponse>& promise)
{
    const auto response =
        SafeExecute<TResponse>([&] { return promise.GetFuture().GetValue(); });
    if constexpr (std::is_same_v<TResponse, NProto::TError>) {
        return response;
    } else {
        return response.GetError();
    }
}

void CheckCompoundCompletion(ECompoundOperation operation,
                             EChildFailure failure)
{
    std::array<TPromise<NProto::TReadBlocksLocalResponse>, 3> reads;
    std::array<TPromise<NProto::TWriteBlocksLocalResponse>, 3> writes;
    std::array<TPromise<NProto::TZeroBlocksResponse>, 3> zeros;
    std::array<TPromise<NProto::TError>, 3> erases;
    std::array<TGuardedSgList, 3> childSglists;
    ui32 dispatched = 0;
    const auto error = MakeError(E_IO, "original child failure");
    TVector<std::shared_ptr<TTestStorage>> devices;
    auto beforeReturn = [&](ui32 index)
    {
        ++dispatched;
        if (index == 1 && failure == EChildFailure::SynchronousException) {
            throw TServiceError(error.GetCode()) << error.GetMessage();
        }
    };
    for (ui32 i = 0; i != 3; ++i) {
        reads[i] = NewPromise<NProto::TReadBlocksLocalResponse>();
        writes[i] = NewPromise<NProto::TWriteBlocksLocalResponse>();
        zeros[i] = NewPromise<NProto::TZeroBlocksResponse>();
        erases[i] = NewPromise<NProto::TError>();
        auto device = std::make_shared<TTestStorage>(1, 'a');
        device->ReadHandler = [&, i](auto request)
        {
            childSglists[i] = request->Sglist;
            beforeReturn(i);
            return reads[i].GetFuture();
        };
        device->WriteHandler = [&, i](auto request)
        {
            childSglists[i] = request->Sglist;
            beforeReturn(i);
            return writes[i].GetFuture();
        };
        device->ZeroHandler = [&, i](auto)
        {
            beforeReturn(i);
            return zeros[i].GetFuture();
        };
        device->EraseHandler = [&, i]
        {
            beforeReturn(i);
            return erases[i].GetFuture();
        };
        devices.push_back(std::move(device));
    }

    auto storage = CreateTestStorage(devices, CreateMonitoringServiceStub());
    TString buffer(3 * DefaultBlockSize, 'x');
    TGuardedSgList sglist(CreateSgListFromBuffer(buffer));
    TFuture<NProto::TError> result;
    switch (operation) {
        case ECompoundOperation::Read: {
            auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
            request->SetBlocksCount(3);
            request->SetBlockSize(DefaultBlockSize);
            request->Sglist = sglist;
            result =
                storage->ReadBlocksLocal(MakeIntrusive<TCallContext>(), request)
                    .Apply([](const auto& f)
                           { return f.GetValue().GetError(); });
            break;
        }
        case ECompoundOperation::Write: {
            auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
            request->BlocksCount = 3;
            request->SetBlockSize(DefaultBlockSize);
            request->Sglist = sglist;
            result =
                storage
                    ->WriteBlocksLocal(MakeIntrusive<TCallContext>(), request)
                    .Apply([](const auto& f)
                           { return f.GetValue().GetError(); });
            break;
        }
        case ECompoundOperation::Zero: {
            auto request = std::make_shared<NProto::TZeroBlocksRequest>();
            request->SetBlocksCount(3);
            result = storage->ZeroBlocks(MakeIntrusive<TCallContext>(), request)
                         .Apply([](const auto& f)
                                { return f.GetValue().GetError(); });
            break;
        }
        case ECompoundOperation::Erase:
            result =
                storage->EraseDevice(NProto::DEVICE_ERASE_METHOD_ZERO_FILL);
            break;
    }
    ui32 completed = 0;
    result.Subscribe(
        [&](const auto&)
        {
            // This runs inline in the last child's completion callback.
            sglist.Close();
            ++completed;
        });
    UNIT_ASSERT_VALUES_EQUAL(dispatched, 3);
    UNIT_ASSERT(!result.IsReady());
    auto finish =
        [&](ui32 i, const NProto::TError& childError, bool exceptional)
    {
        switch (operation) {
            case ECompoundOperation::Read:
                CompleteChild(reads[i], childError, exceptional);
                break;
            case ECompoundOperation::Write:
                CompleteChild(writes[i], childError, exceptional);
                break;
            case ECompoundOperation::Zero:
                CompleteChild(zeros[i], childError, exceptional);
                break;
            case ECompoundOperation::Erase:
                CompleteChild(erases[i], childError, exceptional);
                break;
        }
    };
    if (failure != EChildFailure::SynchronousException) {
        finish(1, failure == EChildFailure::Success ? NProto::TError{} : error,
               failure == EChildFailure::Exception);
    }
    UNIT_ASSERT(!result.IsReady());
    finish(0, {}, false);
    UNIT_ASSERT(!result.IsReady());
    const auto success = MakeError(S_OK, "unchanged child success");
    finish(2, success, false);
    const auto parentError = result.GetValue(TDuration::Seconds(5));
    UNIT_ASSERT_VALUES_EQUAL(parentError.GetCode(),
                             failure == EChildFailure::Success ? S_OK : E_IO);
    if (failure != EChildFailure::Success) {
        UNIT_ASSERT_VALUES_EQUAL(parentError.GetMessage(), error.GetMessage());
    }
    UNIT_ASSERT_VALUES_EQUAL(completed, 1);
    UNIT_ASSERT(!sglist.Acquire());
    if (operation == ECompoundOperation::Read ||
        operation == ECompoundOperation::Write)
    {
        for (auto& child: childSglists) {
            UNIT_ASSERT(!child.Acquire());
        }
    }
    auto originalError = [&](ui32 i)
    {
        switch (operation) {
            case ECompoundOperation::Read:
                return GetChildError(reads[i]);
            case ECompoundOperation::Write:
                return GetChildError(writes[i]);
            case ECompoundOperation::Zero:
                return GetChildError(zeros[i]);
            case ECompoundOperation::Erase:
                return GetChildError(erases[i]);
        }
        Y_ABORT();
    };
    UNIT_ASSERT_VALUES_EQUAL(originalError(2).GetMessage(),
                             success.GetMessage());
    if (failure == EChildFailure::Error || failure == EChildFailure::Exception)
    {
        UNIT_ASSERT_VALUES_EQUAL(originalError(1).GetMessage(),
                                 error.GetMessage());
    }
}

void CopyDeviceRead(const TTestStorage& storage,
                    const NProto::TReadBlocksLocalRequest& request)
{
    auto guard = request.Sglist.Acquire();
    UNIT_ASSERT(guard);
    const auto size = SgListGetSize(guard.Get());
    SgListCopy(
        {storage.Data.data() + request.GetStartIndex() * DefaultBlockSize,
         size}, guard.Get());
}

void CopyDeviceWrite(TTestStorage& storage,
                     const NProto::TWriteBlocksLocalRequest& request)
{
    auto guard = request.Sglist.Acquire();
    UNIT_ASSERT(guard);
    const auto size = SgListGetSize(guard.Get());
    SgListCopy(
        guard.Get(),
        {storage.Data.Detach() + request.GetStartIndex() * DefaultBlockSize,
         size});
}

enum class ERmwResult
{
    ReadError,
    WriteError,
    Success,
};

void CheckCompoundRmw(bool zero, ERmwResult outcome, bool exceptional)
{
    std::array<TPromise<NProto::TReadBlocksLocalResponse>, 2> reads;
    std::array<TPromise<NProto::TWriteBlocksLocalResponse>, 2> writes;
    std::array<std::shared_ptr<NProto::TReadBlocksLocalRequest>, 2>
        readRequests;
    std::array<std::shared_ptr<NProto::TWriteBlocksLocalRequest>, 2>
        writeRequests;
    std::array<ui32, 2> readCount{};
    std::array<ui32, 2> writeCount{};
    TVector<std::shared_ptr<TTestStorage>> devices;
    for (ui32 i = 0; i != 2; ++i) {
        reads[i] = NewPromise<NProto::TReadBlocksLocalResponse>();
        writes[i] = NewPromise<NProto::TWriteBlocksLocalResponse>();
        auto device = std::make_shared<TTestStorage>(1, i == 0 ? 'A' : 'B');
        device->ReadHandler = [&, i, ptr = device.get()](auto request)
        {
            if (readCount[i]++ == 0) {
                readRequests[i] = std::move(request);
                return reads[i].GetFuture();
            }
            CopyDeviceRead(*ptr, *request);
            return MakeFuture<NProto::TReadBlocksLocalResponse>();
        };
        device->WriteHandler = [&, i, ptr = device.get()](auto request)
        {
            if (outcome != ERmwResult::ReadError && writeCount[i]++ == 0) {
                writeRequests[i] = std::move(request);
                return writes[i].GetFuture();
            }
            CopyDeviceWrite(*ptr, *request);
            return MakeFuture<NProto::TWriteBlocksLocalResponse>();
        };
        devices.push_back(std::move(device));
    }
    auto storage = CreateTestStorage(devices, CreateMonitoringServiceStub());
    auto handler = CreateDefaultDeviceHandlerFactory()->CreateDeviceHandler(
        TDeviceHandlerParams{.Storage = storage,
                             .BlockSize = DefaultBlockSize});
    TString originalBuffer(2 * DefaultBlockSize - 2, 'x');
    TString nextBuffer(originalBuffer.size(), 'n');
    TGuardedSgList originalSglist(
        {{originalBuffer.data(), originalBuffer.size()}});
    TGuardedSgList nextSglist({{nextBuffer.data(), nextBuffer.size()}});
    ui64 nowNs = 0;
    TIoDepthTracker depth(1, [&] { return nowNs; });
    ui32 completed = 0;
    auto track = [&](const auto& future)
    {
        return future.Apply(
            [&](const auto& f)
            {
                UNIT_ASSERT(depth.Completed(0));
                ++completed;
                return f.GetValue().GetError();
            });
    };
    depth.Started(0);
    auto first =
        zero ? track(handler->Zero(MakeIntrusive<TCallContext>(), 1,
                                   originalBuffer.size()))
             : track(handler->Write(MakeIntrusive<TCallContext>(), 1,
                                    originalBuffer.size(), originalSglist));
    depth.Started(0);
    auto following = track(handler->Write(MakeIntrusive<TCallContext>(), 1,
                                          nextBuffer.size(), nextSglist));
    UNIT_ASSERT(!first.IsReady());
    UNIT_ASSERT(!following.IsReady());
    UNIT_ASSERT_VALUES_EQUAL(readCount[0], 1);
    UNIT_ASSERT_VALUES_EQUAL(readCount[1], 1);
    nowNs = 1'000;
    CopyDeviceRead(*devices[0], *readRequests[0]);
    CompleteChild(reads[0], {});
    UNIT_ASSERT(!first.IsReady());
    UNIT_ASSERT_VALUES_EQUAL(depth.Snapshot().Lanes[0].Current, 2);
    nowNs = 2'000;
    const auto error = MakeError(E_IO, "RMW child failure");
    if (outcome == ERmwResult::ReadError) {
        CompleteChild(reads[1], error, exceptional);
    } else {
        CopyDeviceRead(*devices[1], *readRequests[1]);
        CompleteChild(reads[1], {});
        UNIT_ASSERT(!first.IsReady());
        UNIT_ASSERT(!following.IsReady());
        UNIT_ASSERT(writeRequests[0]);
        UNIT_ASSERT(writeRequests[1]);
        nowNs = 3'000;
        CopyDeviceWrite(*devices[0], *writeRequests[0]);
        CompleteChild(writes[0], {});
        UNIT_ASSERT(!first.IsReady());
        nowNs = 4'000;
        if (outcome == ERmwResult::WriteError) {
            CompleteChild(writes[1], error, exceptional);
        } else {
            CopyDeviceWrite(*devices[1], *writeRequests[1]);
            CompleteChild(writes[1], {});
        }
    }
    const auto response = first.GetValue(TDuration::Seconds(5));
    UNIT_ASSERT_VALUES_EQUAL(response.GetCode(),
                             outcome == ERmwResult::Success ? S_OK : E_IO);
    if (outcome != ERmwResult::Success) {
        UNIT_ASSERT_VALUES_EQUAL(response.GetMessage(), error.GetMessage());
    }
    UNIT_ASSERT(!HasError(following.GetValue(TDuration::Seconds(5))));
    nowNs = 10'000;
    const auto snapshot = depth.Snapshot();
    UNIT_ASSERT(snapshot.Continuous);
    UNIT_ASSERT_VALUES_EQUAL(snapshot.Lanes[0].Current, 0);
    UNIT_ASSERT_VALUES_EQUAL(snapshot.Lanes[0].IntegralUs,
                             outcome == ERmwResult::ReadError ? 4 : 8);
    UNIT_ASSERT_VALUES_EQUAL(completed, 2);
    UNIT_ASSERT_VALUES_EQUAL(devices[0]->Data, TString("Annnnnnn"));
    UNIT_ASSERT_VALUES_EQUAL(devices[1]->Data, TString("nnnnnnnB"));
    UNIT_ASSERT(!readRequests[0]->Sglist.Acquire());
    UNIT_ASSERT(!readRequests[1]->Sglist.Acquire());
    if (outcome != ERmwResult::ReadError) {
        UNIT_ASSERT(!writeRequests[0]->Sglist.Acquire());
        UNIT_ASSERT(!writeRequests[1]->Sglist.Acquire());
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCompoundStorageTest)
{
    Y_UNIT_TEST(ShouldCompleteAllChildrenAndReleaseParentGuardBeforeCallbacks)
    {
        for (const auto operation:
             {ECompoundOperation::Read, ECompoundOperation::Write,
              ECompoundOperation::Zero, ECompoundOperation::Erase})
        {
            for (const auto failure:
                 {EChildFailure::Success, EChildFailure::Error,
                  EChildFailure::Exception,
                  EChildFailure::SynchronousException})
            {
                CheckCompoundCompletion(operation, failure);
            }
        }
    }

    Y_UNIT_TEST(
        ShouldFinishIoDepthForCompoundRmwAndUnblockFollowingModification)
    {
        for (const bool zero: {false, true}) {
            for (const auto outcome:
                 {ERmwResult::ReadError, ERmwResult::WriteError,
                  ERmwResult::Success})
            {
                CheckCompoundRmw(zero, outcome, false);
                if (outcome != ERmwResult::Success) {
                    CheckCompoundRmw(zero, outcome, true);
                }
            }
        }
    }

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
                EHistogramCounterOption::ReportMultipleCounters,
                {}),
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
            request->SetBlockSize(DefaultBlockSize);
            request->Sglist.Close();

            auto response = storage->WriteBlocksLocal(
                MakeIntrusive<TCallContext>(),
                std::move(request)).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(E_CANCELLED, response.GetError().GetCode());
        }

        {
            auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
            request->SetBlocksCount(100);
            request->SetBlockSize(DefaultBlockSize);
            request->Sglist.Close();

            auto response = storage->ReadBlocksLocal(
                MakeIntrusive<TCallContext>(),
                std::move(request)).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(E_CANCELLED, response.GetError().GetCode());
        }
    }
}

}   // namespace NCloud::NBlockStore::NServer
