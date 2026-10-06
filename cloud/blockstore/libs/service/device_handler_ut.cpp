#include "device_handler.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/diagnostics/critical_events_init.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/storage_test.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/sglist.h>
#include <cloud/storage/core/libs/common/sglist_test.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <array>
#include <exception>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

auto SetupCriticalEvents()
{
    NMonitoring::TDynamicCountersPtr counters =
        new NMonitoring::TDynamicCounters();
    InitCriticalEventsCounter(counters);
    return counters;
}

////////////////////////////////////////////////////////////////////////////////
class TTestEnvironment
{
private:
    const ui64 BlocksCount;
    const ui32 BlockSize;
    const ui32 SectorSize;

    TSgList SgList;
    TVector<TString> Blocks;
    TPromise<void> WriteTrigger;
    IDeviceHandlerPtr DeviceHandler;

    TVector<TFuture<NProto::TError>> Futures;

    ui32 ReadRequestCount = 0;
    ui32 WriteRequestCount = 0;
    ui32 ZeroRequestCount = 0;

    ui32 ZeroedBlocksCount = 0;

public:
    TTestEnvironment(
            ui64 blocksCount,
            ui32 blockSize,
            ui32 sectorsPerBlock,
            ui32 maxBlockCount = 1024,
            bool unalignedRequestsDisabled = false,
            ui32 maxZeroBlocksSubRequestSize = 0)
        : BlocksCount(blocksCount)
        , BlockSize(blockSize)
        , SectorSize(BlockSize / sectorsPerBlock)
    {
        UNIT_ASSERT(SectorSize * sectorsPerBlock == BlockSize);

        SgList = ResizeBlocks(Blocks, BlocksCount, TString(BlockSize, '0'));
        WriteTrigger = NewPromise<void>();

        auto testStorage = std::make_shared<TTestStorage>();
        testStorage->ReadBlocksLocalHandler =
            [&] (TCallContextPtr ctx, std::shared_ptr<NProto::TReadBlocksLocalRequest> request) {
                ++ReadRequestCount;
                ctx->AddTime(EProcessingStage::Postponed, TDuration::Seconds(1));

                auto startIndex = request->GetStartIndex();
                auto guard = request->Sglist.Acquire();
                const auto& dst = guard.Get();

                for (const auto& buffer: dst) {
                    Y_ABORT_UNLESS(buffer.Size() % BlockSize == 0);
                }

                auto src = SgList;
                src.erase(src.begin(), src.begin() + startIndex);
                auto sz = SgListCopy(src, dst);
                UNIT_ASSERT(sz == request->GetBlocksCount() * BlockSize);

                return MakeFuture(NProto::TReadBlocksLocalResponse());
            };
        testStorage->WriteBlocksLocalHandler =
            [&] (TCallContextPtr ctx, std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) {
                ++WriteRequestCount;
                ctx->AddTime(EProcessingStage::Postponed, TDuration::Seconds(10));

                auto future = WriteTrigger.GetFuture();
                return future.Apply([=, this] (const auto& f) {
                    Y_UNUSED(f);

                    auto startIndex = request->GetStartIndex();
                    auto guard = request->Sglist.Acquire();
                    const auto& src = guard.Get();

                    for (const auto& buffer: src) {
                        Y_ABORT_UNLESS(buffer.Size() % BlockSize == 0);
                    }

                    auto dst = SgList;
                    dst.erase(dst.begin(), dst.begin() + startIndex);
                    auto sz = SgListCopy(src, dst);
                    UNIT_ASSERT(sz == request->BlocksCount * BlockSize);

                    return NProto::TWriteBlocksLocalResponse();
                });
            };
        testStorage->ZeroBlocksHandler =
            [&] (TCallContextPtr ctx, std::shared_ptr<NProto::TZeroBlocksRequest> request) {
                ++ZeroRequestCount;
                ZeroedBlocksCount += request->GetBlocksCount();
                ctx->AddTime(EProcessingStage::Postponed, TDuration::Seconds(100));

                auto future = WriteTrigger.GetFuture();
                return future.Apply([=, this] (const auto& f) {
                    Y_UNUSED(f);

                    auto startIndex = request->GetStartIndex();
                    TSgList src(
                        request->GetBlocksCount(),
                        TBlockDataRef::CreateZeroBlock(BlockSize));

                    auto dst = SgList;
                    dst.erase(dst.begin(), dst.begin() + startIndex);
                    auto sz = SgListCopy(src, dst);
                    UNIT_ASSERT(sz == request->GetBlocksCount() * BlockSize);

                    return NProto::TZeroBlocksResponse();
                });
            };

        auto factory =
            CreateDeviceHandlerFactoryForTesting(maxBlockCount * BlockSize);
        DeviceHandler = factory->CreateDeviceHandler(
            TDeviceHandlerParams{
                .Storage = std::move(testStorage),
                .DiskId = "disk1",
                .ClientId = "testClientId",
                .BlockSize = BlockSize,
                .MaxZeroBlocksSubRequestSize = maxZeroBlocksSubRequestSize,
                .UnalignedRequestsDisabled = unalignedRequestsDisabled,
                .StorageMediaKind = NProto::STORAGE_MEDIA_SSD_NONREPLICATED});
    }

    TCallContextPtr WriteSectors(ui64 firstSector, ui64 totalSectors, char data)
    {
        auto ctx = MakeIntrusive<TCallContext>();
        auto buffer = TString(totalSectors * SectorSize, data);
        TSgList sgList;
        for (size_t i = 0; i < totalSectors; ++i) {
            sgList.emplace_back(buffer.data() + i * SectorSize, SectorSize);
        }

        auto future = DeviceHandler->Write(
            ctx,
            firstSector * SectorSize,
            totalSectors * SectorSize,
            TGuardedSgList(sgList))
        .Apply([buf = std::move(buffer)] (const auto& f) {
            Y_UNUSED(buf);

            return f.GetValue().GetError();
        });

        Futures.push_back(future);
        return ctx;
    }

    TCallContextPtr ZeroSectors(ui64 firstSector, ui64 totalSectors)
    {
        auto ctx = MakeIntrusive<TCallContext>();
        auto future = DeviceHandler->Zero(
            ctx,
            firstSector * SectorSize,
            totalSectors * SectorSize)
        .Apply([] (const auto& f) {
            return f.GetValue().GetError();
        });

        Futures.push_back(future);
        return ctx;
    }

    std::pair<TPromise<void>, TVector<TFuture<NProto::TError>>> TakeWriteTrigger()
    {
        TPromise<void> result = NewPromise<void>();
        WriteTrigger.Swap(result);
        return std::make_pair(std::move(result), std::move(Futures));
    }

    void RunWriteService()
    {
        WriteTrigger.SetValue();

        for (const auto& future: Futures) {
            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));
        }
    }

    void ReadSectorsAndCheck(
        ui64 firstSector,
        ui64 totalSectors,
        const TString& expected)
    {
        UNIT_ASSERT(expected.size() == totalSectors);

        TString buffer = TString::Uninitialized(totalSectors * SectorSize);
        TSgList sgList;
        for (size_t i = 0; i < totalSectors; ++i) {
            sgList.emplace_back(buffer.data() + i * SectorSize, SectorSize);
        }
        TString checkpointId;

        auto future = DeviceHandler->Read(
            MakeIntrusive<TCallContext>(),
            firstSector * SectorSize,
            totalSectors * SectorSize,
            TGuardedSgList(sgList),
            checkpointId);

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(!HasError(response));

        TString read;
        read.resize(expected.size(), 0);
        const char* ptr = buffer.data();
        bool allOk = true;
        for (size_t i = 0; i != expected.size(); ++i) {
            char c = expected[i];
            read[i] = *ptr == 0 ? 'Z' : *ptr;
            for (size_t j = 0; j < SectorSize; ++j) {
                if (c == 'Z') {
                    allOk = allOk && *ptr == 0;
                } else {
                    allOk = allOk && *ptr == c;
                }
                ++ptr;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(expected, read);
        UNIT_ASSERT(allOk);
    }

    ui32 GetReadRequestCount() const {
        return ReadRequestCount;
    }

    ui32 GetWriteRequestCount() const {
        return WriteRequestCount;
    }

    ui32 GetZeroRequestCount() const {
        return ZeroRequestCount;
    }

    void ResetRequestCounters()
    {
        ReadRequestCount = 0;
        WriteRequestCount = 0;
        ZeroRequestCount = 0;
        ZeroedBlocksCount = 0;
    }

    ui32 GetZeroedBlocksCount() const
    {
        return ZeroedBlocksCount;
    }
};

enum class EStorageMethod
{
    Read,
    Write,
    Zero,
};

struct TModificationFailureScenario
{
    bool Zero;
    bool Aligned;
    EStorageMethod FailingMethod;
};

struct TTestStorageWithTrackedBuffers final: TTestStorage
{
    std::function<void()> BufferReleaseHandler;

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return TStorageBuffer(
            new char[bytesCount],
            [handler = BufferReleaseHandler](char* buffer)
            {
                handler();
                delete[] buffer;
            });
    }
};

constexpr std::array<TModificationFailureScenario, 6>
    ModificationFailureScenarios{{
        {false, true, EStorageMethod::Write},
        {false, false, EStorageMethod::Read},
        {false, false, EStorageMethod::Write},
        {true, true, EStorageMethod::Zero},
        {true, false, EStorageMethod::Read},
        {true, false, EStorageMethod::Write},
    }};

void CheckModificationFailure(const TModificationFailureScenario& scenario,
                              bool asynchronous, bool postponed)
{
    const auto storageError = MakeError(E_REJECTED, "storage failure");
    const TServiceError storageException(storageError);
    const ui32 blockSize = DefaultBlockSize;
    const auto failureContext =
        TStringBuilder() << "zero=" << scenario.Zero
                         << ", aligned=" << scenario.Aligned << ", method="
                         << static_cast<ui32>(scenario.FailingMethod)
                         << ", asynchronous=" << asynchronous
                         << ", postponed=" << postponed;

    auto readPromise = NewPromise<NProto::TReadBlocksLocalResponse>();
    auto writePromise = NewPromise<NProto::TWriteBlocksLocalResponse>();
    auto zeroPromise = NewPromise<NProto::TZeroBlocksResponse>();
    auto blockerPromise = NewPromise<NProto::TWriteBlocksLocalResponse>();
    bool blockWrite = postponed;
    bool failureArmed = !postponed;
    ui32 failures = 0;
    ui32 successfulWrites = 0;
    TVector<TGuardedSgList> retainedRMWBuffers;
    ui32 buffersReleased = 0;
    bool rmwAccessClosedBeforeRelease = true;

    auto fail = [&](auto& promise)
    {
        failureArmed = false;
        ++failures;
        if (!asynchronous) {
            throw storageException;
        }
        return promise.GetFuture();
    };

    auto storage = std::make_shared<TTestStorageWithTrackedBuffers>();
    storage->BufferReleaseHandler = [&]
    {
        ++buffersReleased;
        for (const auto& retained: retainedRMWBuffers) {
            if (retained.Acquire()) {
                rmwAccessClosedBeforeRelease = false;
            }
        }
    };
    storage->ReadBlocksLocalHandler =
        [&](TCallContextPtr,
            std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
    {
        if (failureArmed && scenario.FailingMethod == EStorageMethod::Read) {
            retainedRMWBuffers.push_back(request->Sglist);
            return fail(readPromise);
        }
        return MakeFuture<NProto::TReadBlocksLocalResponse>();
    };
    storage->WriteBlocksLocalHandler =
        [&](TCallContextPtr,
            std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
    {
        if (blockWrite) {
            blockWrite = false;
            return blockerPromise.GetFuture();
        }
        if (failureArmed && scenario.FailingMethod == EStorageMethod::Write) {
            if (!scenario.Aligned) {
                retainedRMWBuffers.push_back(request->Sglist);
            }
            return fail(writePromise);
        }
        ++successfulWrites;
        return MakeFuture<NProto::TWriteBlocksLocalResponse>();
    };
    storage->ZeroBlocksHandler =
        [&](TCallContextPtr, std::shared_ptr<NProto::TZeroBlocksRequest>)
    {
        if (failureArmed && scenario.FailingMethod == EStorageMethod::Zero) {
            return fail(zeroPromise);
        }
        return MakeFuture<NProto::TZeroBlocksResponse>();
    };

    auto deviceHandler =
        CreateDeviceHandlerFactoryForTesting(blockSize)->CreateDeviceHandler(
            TDeviceHandlerParams{
                .Storage = storage,
                .BlockSize = blockSize,
                .StorageMediaKind = NProto::STORAGE_MEDIA_SSD});
    auto buffer = TString(blockSize, 'a');
    TGuardedSgList callerSgList({{buffer.data(), buffer.size()}});
    auto write = [&](bool aligned)
    {
        const ui64 length = aligned ? blockSize : 1;
        return deviceHandler->Write(
            MakeIntrusive<TCallContext>(), aligned ? 0 : 1, length,
            callerSgList.Create({{buffer.data(), length}}));
    };

    TFuture<NProto::TWriteBlocksLocalResponse> blocker;
    if (postponed) {
        // An unaligned modification blocks both aligned and unaligned requests
        // on the same block.
        blocker = write(false);
        UNIT_ASSERT_C(!blocker.IsReady(), failureContext);
        failureArmed = true;
    }

    TFuture<NProto::TError> failed;
    if (scenario.Zero) {
        failed =
            deviceHandler
                ->Zero(MakeIntrusive<TCallContext>(), scenario.Aligned ? 0 : 1,
                       scenario.Aligned ? blockSize : 1)
                .Apply([](const auto& f) { return f.GetValue().GetError(); });
    } else {
        failed =
            write(scenario.Aligned)
                .Apply([](const auto& f) { return f.GetValue().GetError(); });
    }
    // Exercise both request queues: an aligned failure blocks unaligned I/O,
    // and an unaligned failure blocks aligned I/O.
    auto following = write(!scenario.Aligned);
    if (postponed) {
        UNIT_ASSERT_C(!failed.IsReady(), failureContext);
        UNIT_ASSERT_C(!following.IsReady(), failureContext);
        blockerPromise.SetValue(NProto::TWriteBlocksLocalResponse());
        UNIT_ASSERT_C(blocker.IsReady(), failureContext);
        UNIT_ASSERT_C(!HasError(blocker.GetValue()), failureContext);
    }

    UNIT_ASSERT_VALUES_EQUAL_C(1, failures, failureContext);
    if (asynchronous) {
        UNIT_ASSERT_C(!failed.IsReady(), failureContext);
        UNIT_ASSERT_C(!following.IsReady(), failureContext);
        auto exception = std::make_exception_ptr(storageException);
        switch (scenario.FailingMethod) {
            case EStorageMethod::Read:
                readPromise.SetException(exception);
                break;
            case EStorageMethod::Write:
                writePromise.SetException(exception);
                break;
            case EStorageMethod::Zero:
                zeroPromise.SetException(exception);
                break;
        }
    }

    UNIT_ASSERT_C(failed.IsReady(), failureContext);
    const auto& error = failed.GetValue();
    UNIT_ASSERT_VALUES_EQUAL_C(storageError.GetCode(), error.GetCode(),
                               failureContext);
    UNIT_ASSERT_VALUES_EQUAL_C(storageException.GetMessage(),
                               error.GetMessage(), failureContext);
    UNIT_ASSERT_C(following.IsReady(), failureContext);
    UNIT_ASSERT_C(!HasError(following.GetValue()), failureContext);
    UNIT_ASSERT_VALUES_EQUAL_C(1, successfulWrites, failureContext);
    UNIT_ASSERT_VALUES_EQUAL_C(scenario.Aligned ? 0 : 1,
                               retainedRMWBuffers.size(), failureContext);
    for (const auto& retained: retainedRMWBuffers) {
        // Storage may keep an sglist after failing, but cannot acquire the
        // request-owned buffer after the request completes and frees it.
        UNIT_ASSERT_C(!retained.Acquire(), failureContext);
    }
    UNIT_ASSERT_C(callerSgList.Acquire(), failureContext);
    UNIT_ASSERT_C(buffersReleased > 0, failureContext);
    UNIT_ASSERT_C(rmwAccessClosedBeforeRelease, failureContext);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDeviceHandlerTest)
{
    Y_UNIT_TEST(ShouldNotifySubscribersOnAsynchronousStorageExceptions)
    {
        auto counters = SetupCriticalEvents();
        auto criticalEvent = counters->GetCounter(
            "AppCriticalEvents/ErrorWasSentToTheGuestForReliableDisk", true);
        auto storage = std::make_shared<TTestStorage>();
        auto readPromise = NewPromise<NProto::TReadBlocksLocalResponse>();
        auto writePromise = NewPromise<NProto::TWriteBlocksLocalResponse>();
        auto zeroPromise = NewPromise<NProto::TZeroBlocksResponse>();
        storage->ReadBlocksLocalHandler =
            [&](TCallContextPtr,
                std::shared_ptr<NProto::TReadBlocksLocalRequest>)
        {
            return readPromise.GetFuture();
        };
        storage->WriteBlocksLocalHandler =
            [&](TCallContextPtr,
                std::shared_ptr<NProto::TWriteBlocksLocalRequest>)
        {
            return writePromise.GetFuture();
        };
        storage->ZeroBlocksHandler =
            [&](TCallContextPtr, std::shared_ptr<NProto::TZeroBlocksRequest>)
        {
            return zeroPromise.GetFuture();
        };
        auto deviceHandler =
            CreateDeviceHandlerFactoryForTesting(DefaultBlockSize)
                ->CreateDeviceHandler(TDeviceHandlerParams{
                    .Storage = storage,
                    .BlockSize = DefaultBlockSize,
                    .UnalignedRequestsDisabled = true,
                    .StorageMediaKind = NProto::STORAGE_MEDIA_SSD});
        auto buffer = TString(DefaultBlockSize, 'a');
        TGuardedSgList sgList({{buffer.data(), buffer.size()}});

        auto read = deviceHandler->Read(MakeIntrusive<TCallContext>(), 0,
                                        DefaultBlockSize, sgList,
                                        {});
        auto write = deviceHandler->Write(MakeIntrusive<TCallContext>(), 0,
                                          DefaultBlockSize, sgList);
        auto zero = deviceHandler->Zero(MakeIntrusive<TCallContext>(), 0,
                                        DefaultBlockSize);
        UNIT_ASSERT(!read.IsReady());
        UNIT_ASSERT(!write.IsReady());
        UNIT_ASSERT(!zero.IsReady());
        ui32 notified = 0;
        read.Subscribe([&](const auto&) { ++notified; });
        write.Subscribe([&](const auto&) { ++notified; });
        zero.Subscribe([&](const auto&) { ++notified; });

        const auto error =
            MakeError(E_REJECTED, "asynchronous storage failure");
        auto exception = std::make_exception_ptr(TServiceError(error));
        // Resolve pending promises after all callbacks have been registered.
        // A throwing diagnostic subscriber would stop dispatch before these
        // completion subscribers are notified.
        readPromise.SetException(exception);
        writePromise.SetException(exception);
        zeroPromise.SetException(exception);

        UNIT_ASSERT_VALUES_EQUAL(3, notified);
        UNIT_ASSERT_VALUES_EQUAL(3, criticalEvent->Val());
        UNIT_ASSERT_VALUES_EQUAL(error.GetCode(),
                                 ExtractResponse(read).GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(error.GetCode(),
                                 ExtractResponse(write).GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(error.GetCode(),
                                 ExtractResponse(zero).GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldPreserveSharedStorageResponses)
    {
        auto counters = SetupCriticalEvents();
        for (bool unalignedRequestsDisabled: {false, true}) {
            for (bool errorResponse: {false, true}) {
                auto storage = std::make_shared<TTestStorage>();
                auto readPromise =
                    NewPromise<NProto::TReadBlocksLocalResponse>();
                auto writePromise =
                    NewPromise<NProto::TWriteBlocksLocalResponse>();
                auto zeroPromise = NewPromise<NProto::TZeroBlocksResponse>();
                storage->ReadBlocksLocalHandler =
                    [&](TCallContextPtr,
                        std::shared_ptr<NProto::TReadBlocksLocalRequest>)
                {
                    return readPromise.GetFuture();
                };
                storage->WriteBlocksLocalHandler =
                    [&](TCallContextPtr,
                        std::shared_ptr<NProto::TWriteBlocksLocalRequest>)
                {
                    return writePromise.GetFuture();
                };
                storage->ZeroBlocksHandler =
                    [&](TCallContextPtr,
                        std::shared_ptr<NProto::TZeroBlocksRequest>)
                {
                    return zeroPromise.GetFuture();
                };
                auto deviceHandler =
                    CreateDeviceHandlerFactoryForTesting(DefaultBlockSize)
                        ->CreateDeviceHandler(TDeviceHandlerParams{
                            .Storage = storage,
                            .BlockSize = DefaultBlockSize,
                            .UnalignedRequestsDisabled =
                                unalignedRequestsDisabled,
                            .StorageMediaKind = NProto::STORAGE_MEDIA_SSD});
                auto buffer = TString(DefaultBlockSize, 'a');
                TGuardedSgList sgList({{buffer.data(), buffer.size()}});
                auto read = deviceHandler->Read(MakeIntrusive<TCallContext>(),
                                                0, DefaultBlockSize, sgList,
                                                {});
                auto write = deviceHandler->Write(MakeIntrusive<TCallContext>(),
                                                  0, DefaultBlockSize, sgList);
                auto zero = deviceHandler->Zero(MakeIntrusive<TCallContext>(),
                                                0, DefaultBlockSize);

                auto prepareResponse = [errorResponse](auto& response)
                {
                    response.MutableHeaders()->MutableThrottler()->SetDelay(
                        123);
                    if (errorResponse) {
                        *response.MutableError() =
                            MakeError(E_IO, "original storage error message");
                    }
                };
                NProto::TReadBlocksLocalResponse readResponse;
                NProto::TWriteBlocksLocalResponse writeResponse;
                NProto::TZeroBlocksResponse zeroResponse;
                prepareResponse(readResponse);
                prepareResponse(writeResponse);
                prepareResponse(zeroResponse);
                readPromise.SetValue(readResponse);
                writePromise.SetValue(writeResponse);
                zeroPromise.SetValue(zeroResponse);

                // Both the caller and other consumers of the shared storage
                // future must retain error details and successful metadata.
                UNIT_ASSERT_VALUES_EQUAL(readResponse.SerializeAsString(),
                                         read.GetValue().SerializeAsString());
                UNIT_ASSERT_VALUES_EQUAL(writeResponse.SerializeAsString(),
                                         write.GetValue().SerializeAsString());
                UNIT_ASSERT_VALUES_EQUAL(zeroResponse.SerializeAsString(),
                                         zero.GetValue().SerializeAsString());
                UNIT_ASSERT_VALUES_EQUAL(
                    readResponse.SerializeAsString(),
                    readPromise.GetFuture().GetValue().SerializeAsString());
                UNIT_ASSERT_VALUES_EQUAL(
                    writeResponse.SerializeAsString(),
                    writePromise.GetFuture().GetValue().SerializeAsString());
                UNIT_ASSERT_VALUES_EQUAL(
                    zeroResponse.SerializeAsString(),
                    zeroPromise.GetFuture().GetValue().SerializeAsString());
            }
        }
    }

    Y_UNIT_TEST(ShouldUnregisterFailedModifications)
    {
        auto counters = SetupCriticalEvents();
        for (bool asynchronous: {false, true}) {
            for (const auto& scenario: ModificationFailureScenarios) {
                CheckModificationFailure(scenario, asynchronous, false);
            }
        }
    }

    Y_UNIT_TEST(ShouldUnregisterFailedPostponedModifications)
    {
        auto counters = SetupCriticalEvents();
        for (bool asynchronous: {false, true}) {
            for (const auto& scenario: ModificationFailureScenarios) {
                CheckModificationFailure(scenario, asynchronous, true);
            }
        }
    }

    Y_UNIT_TEST(ShouldHandleUnalignedReadRequests)
    {
        TTestEnvironment env(2, DefaultBlockSize, 8);

        env.WriteSectors(0, 4, 'a');
        env.WriteSectors(4, 4, 'b');
        env.WriteSectors(8, 4, 'c');
        env.WriteSectors(12, 4, 'd');

        env.RunWriteService();
        env.ReadSectorsAndCheck(0, 16, "aaaabbbbccccdddd");
        env.ReadSectorsAndCheck(8, 1, "c");
        env.ReadSectorsAndCheck(3, 4, "abbb");
        env.ReadSectorsAndCheck(7, 5, "bcccc");
        env.ReadSectorsAndCheck(3, 11, "abbbbccccdd");
    }

    Y_UNIT_TEST(ShouldHandleReadModifyWriteRequests)
    {
        TTestEnvironment env(2, DefaultBlockSize, 8);

        env.WriteSectors(1, 2, 'a');
        env.WriteSectors(4, 4, 'b');
        env.ZeroSectors(5, 2);

        env.RunWriteService();
        env.ReadSectorsAndCheck(0, 8, "0aa0bZZb");
    }

    Y_UNIT_TEST(ShouldHandleAlignedAndRMWRequests)
    {
        TTestEnvironment env(2, DefaultBlockSize, 8);

        env.ZeroSectors(0, 8);
        env.WriteSectors(1, 1, 'a');
        env.WriteSectors(5, 2, 'b');

        env.RunWriteService();
        env.ReadSectorsAndCheck(0, 8, "ZaZZZbbZ");
    }

    Y_UNIT_TEST(ShouldHandleRMWAndAlignedRequests)
    {
        TTestEnvironment env(2, DefaultBlockSize, 8);

        env.ZeroSectors(1, 1);
        env.WriteSectors(5, 2, 'a');
        env.WriteSectors(0, 8, 'b');

        env.RunWriteService();
        env.ReadSectorsAndCheck(0, 8, "bbbbbbbb");
    }

    Y_UNIT_TEST(ShouldHandleComplicatedRMWRequests)
    {
        TTestEnvironment env(2, DefaultBlockSize, 8);

        env.WriteSectors(5, 2, 'a');
        env.ZeroSectors(13, 2);
        env.WriteSectors(0, 16, 'c');
        env.WriteSectors(1, 2, 'd');
        env.ZeroSectors(6, 4);
        env.WriteSectors(12, 3, 'f');
        env.WriteSectors(0, 8, 'g');

        env.RunWriteService();
        env.ReadSectorsAndCheck(0, 16, "ggggggggZZccfffc");
    }

    Y_UNIT_TEST(ShouldSliceHugeZeroRequest)
    {
        const auto diskId = "disk1";
        const auto clientId = "testClientId";
        const ui32 blockSize = DefaultBlockSize;
        const ui64 deviceBlocksCount = 8 * 1024;
        const ui64 blocksCountLimit = deviceBlocksCount / 4;
        const ui32 maxZeroBlocksSubRequestSize = 0;

        auto storage = std::make_shared<TTestStorage>();

        auto factory =
            CreateDeviceHandlerFactoryForTesting(blocksCountLimit * blockSize);
        auto deviceHandler = factory->CreateDeviceHandler(
            TDeviceHandlerParams{
                .Storage = storage,
                .DiskId = diskId,
                .ClientId = clientId,
                .BlockSize = blockSize,
                .MaxZeroBlocksSubRequestSize = maxZeroBlocksSubRequestSize,
                .UnalignedRequestsDisabled = false,
                .StorageMediaKind = NProto::STORAGE_MEDIA_SSD_NONREPLICATED});

        std::array<bool, deviceBlocksCount> zeroBlocks;
        for (auto& zeroBlock: zeroBlocks) {
            zeroBlock = false;
        }

        ui32 requestCounter = 0;

        storage->ZeroBlocksHandler = [&] (
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TZeroBlocksRequest> request)
        {
            Y_UNUSED(callContext);

            UNIT_ASSERT(request->GetHeaders().GetClientId() == clientId);
            UNIT_ASSERT(request->GetBlocksCount() <= blocksCountLimit);
            UNIT_ASSERT(request->GetStartIndex() + request->GetBlocksCount() <= deviceBlocksCount);

            for (ui32 i = 0; i < request->GetBlocksCount(); ++i) {
                auto index = request->GetStartIndex() + i;
                auto& zeroBlock = zeroBlocks[index];

                UNIT_ASSERT(!zeroBlock);
                zeroBlock = true;
            }

            ++requestCounter;
            return MakeFuture<NProto::TZeroBlocksResponse>();
        };

        ui64 startIndex = 3;
        ui64 blocksCount = deviceBlocksCount - 9;

        auto future = deviceHandler->Zero(
            MakeIntrusive<TCallContext>(),
            startIndex * blockSize,
            blocksCount * blockSize);

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(!HasError(response));

        UNIT_ASSERT(requestCounter > 1);

        for (ui64 i = 0; i < deviceBlocksCount; ++i) {
            const auto& zeroBlock = zeroBlocks[i];
            auto contains = (startIndex <= i && i < (startIndex + blocksCount));
            UNIT_ASSERT(zeroBlock == contains);
        }
    }

    Y_UNIT_TEST(ShouldNotSliceZeroRequest)
    {
        const auto diskId = "disk1";
        const auto clientId = "testClientId";
        const ui32 blockSize = DefaultBlockSize;
        const ui64 deviceBlocksCount = 8 * 1024;
        const ui64 blocksCountLimit = deviceBlocksCount / 4;
        const ui32 maxZeroBlocksSubRequestSize = 512 * 1024 * 1024;

        auto storage = std::make_shared<TTestStorage>();

        auto factory =
            CreateDeviceHandlerFactoryForTesting(blocksCountLimit * blockSize);
        auto deviceHandler = factory->CreateDeviceHandler(
            TDeviceHandlerParams{
                .Storage = storage,
                .DiskId = diskId,
                .ClientId = clientId,
                .BlockSize = blockSize,
                .MaxZeroBlocksSubRequestSize = maxZeroBlocksSubRequestSize,
                .UnalignedRequestsDisabled = false,
                .StorageMediaKind = NProto::STORAGE_MEDIA_SSD_NONREPLICATED});

        std::array<bool, deviceBlocksCount> zeroBlocks;
        for (auto& zeroBlock: zeroBlocks) {
            zeroBlock = false;
        }

        ui32 requestCounter = 0;

        storage->ZeroBlocksHandler = [&] (
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TZeroBlocksRequest> request)
        {
            Y_UNUSED(callContext);
            Y_UNUSED(request);
            ++requestCounter;
            return MakeFuture<NProto::TZeroBlocksResponse>();
        };

        auto future = deviceHandler->Zero(
            MakeIntrusive<TCallContext>(),
            0,
            deviceBlocksCount * blockSize);

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(!HasError(response));
        UNIT_ASSERT_EQUAL_C(1, requestCounter, requestCounter);
    }

    Y_UNIT_TEST(ShouldHandleAlignedRequestsWhenUnalignedRequestsDisabled)
    {
        const auto diskId = "disk1";
        const auto clientId = "testClientId";
        const ui32 blockSize = DefaultBlockSize;
        const ui32 maxZeroBlocksSubRequestSize = 0;

        auto storage = std::make_shared<TTestStorage>();

        auto factory = CreateDefaultDeviceHandlerFactory();
        auto deviceHandler = factory->CreateDeviceHandler(
            TDeviceHandlerParams{
                .Storage = storage,
                .DiskId = diskId,
                .ClientId = clientId,
                .BlockSize = blockSize,
                .MaxZeroBlocksSubRequestSize = maxZeroBlocksSubRequestSize,
                .UnalignedRequestsDisabled = true,
                .StorageMediaKind = NProto::STORAGE_MEDIA_SSD_NONREPLICATED});

        ui32 startIndex = 42;
        ui32 blocksCount = 17;

        auto buffer = TString::Uninitialized(blocksCount * DefaultBlockSize);
        TSgList sgList{{ buffer.data(), buffer.size() }};

        storage->ReadBlocksLocalHandler =
            [&] (TCallContextPtr ctx, std::shared_ptr<NProto::TReadBlocksLocalRequest> request) {
                Y_UNUSED(ctx);

                UNIT_ASSERT(request->GetHeaders().GetClientId() == clientId);
                UNIT_ASSERT(request->GetStartIndex() == startIndex);
                UNIT_ASSERT(request->GetBlocksCount() == blocksCount);

                return MakeFuture<NProto::TReadBlocksLocalResponse>();
            };

        {
            auto future = deviceHandler->Read(
                MakeIntrusive<TCallContext>(),
                startIndex * blockSize,
                blocksCount * blockSize,
                TGuardedSgList(sgList),
                {});

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));
        }

        storage->WriteBlocksLocalHandler =
            [&] (TCallContextPtr ctx, std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) {
                Y_UNUSED(ctx);

                UNIT_ASSERT(request->GetHeaders().GetClientId() == clientId);
                UNIT_ASSERT(request->GetStartIndex() == startIndex);
                UNIT_ASSERT(request->BlocksCount == blocksCount);

                return MakeFuture<NProto::TWriteBlocksLocalResponse>();
            };

        {
            auto future = deviceHandler->Write(
                MakeIntrusive<TCallContext>(),
                startIndex * blockSize,
                blocksCount * blockSize,
                TGuardedSgList(sgList));

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));
        }

        storage->ZeroBlocksHandler =
            [&] (TCallContextPtr ctx, std::shared_ptr<NProto::TZeroBlocksRequest> request) {
                Y_UNUSED(ctx);

                UNIT_ASSERT(request->GetHeaders().GetClientId() == clientId);
                UNIT_ASSERT(request->GetStartIndex() == startIndex);
                UNIT_ASSERT(request->GetBlocksCount() == blocksCount);

                return MakeFuture<NProto::TZeroBlocksResponse>();
            };

        {
            auto future = deviceHandler->Zero(
                MakeIntrusive<TCallContext>(),
                startIndex * blockSize,
                blocksCount * blockSize);

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));
        }
    }

    Y_UNIT_TEST(ShouldNotHandleUnalignedRequestsWhenUnalignedRequestsDisabled)
    {
        const auto diskId = "disk1";
        const auto clientId = "testClientId";
        const ui32 blockSize = DefaultBlockSize;
        const ui32 maxZeroBlocksSubRequestSize = 0;

        auto storage = std::make_shared<TTestStorage>();

        auto device =
            CreateDefaultDeviceHandlerFactory()->CreateDeviceHandler(
                TDeviceHandlerParams{
                    .Storage = storage,
                    .DiskId = diskId,
                    .ClientId = clientId,
                    .BlockSize = blockSize,
                    .MaxZeroBlocksSubRequestSize = maxZeroBlocksSubRequestSize,
                    .UnalignedRequestsDisabled = true,
                    .StorageMediaKind =
                        NProto::STORAGE_MEDIA_SSD_NONREPLICATED});

        {
            auto future = device->Read(
                MakeIntrusive<TCallContext>(),
                blockSize * 5 / 2,
                blockSize * 8 / 3,
                TGuardedSgList(),
                {});

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(HasError(response));
            UNIT_ASSERT(response.GetError().GetCode() == E_ARGUMENT);
        }

        {
            auto future = device->Write(
                MakeIntrusive<TCallContext>(),
                blockSize * 3,
                blockSize * 7 / 3,
                TGuardedSgList());

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(HasError(response));
            UNIT_ASSERT(response.GetError().GetCode() == E_ARGUMENT);
        }

        {
            auto future = device->Zero(
                MakeIntrusive<TCallContext>(),
                blockSize * 3 / 2,
                blockSize * 4);

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(HasError(response));
            UNIT_ASSERT(response.GetError().GetCode() == E_ARGUMENT);
        }
    }

    void ShouldSliceHugeAlignedRequests(bool unalignedRequestsDisabled)
    {
        TTestEnvironment
            env(24, DefaultBlockSize, 1, 4, unalignedRequestsDisabled);

        env.RunWriteService();

        env.ZeroSectors(0, 24);
        UNIT_ASSERT_VALUES_EQUAL(6, env.GetZeroRequestCount());
        env.WriteSectors(0, 8, 'a');
        env.WriteSectors(8, 8, 'b');
        UNIT_ASSERT_VALUES_EQUAL(4, env.GetWriteRequestCount());

        env.ReadSectorsAndCheck(0, 24, "aaaaaaaabbbbbbbbZZZZZZZZ");
        UNIT_ASSERT_VALUES_EQUAL(6, env.GetReadRequestCount());

        env.ResetRequestCounters();
        env.ZeroSectors(4, 8);
        UNIT_ASSERT_VALUES_EQUAL(2, env.GetZeroRequestCount());
        env.ReadSectorsAndCheck(0, 24, "aaaaZZZZZZZZbbbbZZZZZZZZ");
        UNIT_ASSERT_VALUES_EQUAL(6, env.GetReadRequestCount());
    }

    Y_UNIT_TEST(ShouldSliceHugeAlignedRequestsInAlignedBackend)
    {
       ShouldSliceHugeAlignedRequests(true);
    }

     Y_UNIT_TEST(ShouldSliceHugeAlignedRequestsInUnalignedBackend)
    {
       ShouldSliceHugeAlignedRequests(false);
    }

    Y_UNIT_TEST (ShouldSliceHugeUnalignedRequests)
    {
        TTestEnvironment
            env(24, DefaultBlockSize, 2, 4, false);

        env.RunWriteService();

        // An unaligned request requires the execution of a read-modify-write
        // pattern for first and last blocks with unaligned offsets.
        env.ZeroSectors(1, 46);
        UNIT_ASSERT_VALUES_EQUAL(2, env.GetReadRequestCount());
        UNIT_ASSERT_VALUES_EQUAL(2, env.GetWriteRequestCount());
        UNIT_ASSERT_VALUES_EQUAL(6, env.GetZeroRequestCount());
        UNIT_ASSERT_VALUES_EQUAL(22, env.GetZeroedBlocksCount());
        env.ResetRequestCounters();

        env.ZeroSectors(1, 39);
        UNIT_ASSERT_VALUES_EQUAL(1, env.GetReadRequestCount());
        UNIT_ASSERT_VALUES_EQUAL(1, env.GetWriteRequestCount());
        UNIT_ASSERT_VALUES_EQUAL(5, env.GetZeroRequestCount());
        UNIT_ASSERT_VALUES_EQUAL(19, env.GetZeroedBlocksCount());
        env.ResetRequestCounters();

        env.ZeroSectors(8, 37);
        UNIT_ASSERT_VALUES_EQUAL(1, env.GetReadRequestCount());
        UNIT_ASSERT_VALUES_EQUAL(1, env.GetWriteRequestCount());
        UNIT_ASSERT_VALUES_EQUAL(5, env.GetZeroRequestCount());
        UNIT_ASSERT_VALUES_EQUAL(18, env.GetZeroedBlocksCount());
        env.ResetRequestCounters();

        env.WriteSectors(3, 8, 'a');
        UNIT_ASSERT_VALUES_EQUAL(2, env.GetReadRequestCount());
        UNIT_ASSERT_VALUES_EQUAL(2, env.GetWriteRequestCount());
        env.ResetRequestCounters();

        env.WriteSectors(13, 3, 'b');
        UNIT_ASSERT_VALUES_EQUAL(1, env.GetReadRequestCount());
        UNIT_ASSERT_VALUES_EQUAL(1, env.GetWriteRequestCount());
        env.ResetRequestCounters();

        env.ReadSectorsAndCheck(
            0,
            48,
            "0ZZaaaaaaaaZZbbbZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ0");
        UNIT_ASSERT_VALUES_EQUAL(6, env.GetReadRequestCount());
    }

    void DoShouldSliceHugeZeroRequest(bool requestUnaligned, bool unalignedRequestDisabled)
    {
        const auto diskId = "disk1";
        const auto clientId = "testClientId";
        const ui32 blockSize = DefaultBlockSize;
        const ui64 deviceBlocksCount = 12;
        const ui64 blocksCountLimit = deviceBlocksCount / 4;
        const ui32 maxZeroBlocksSubRequestSize = 0;

        TString device(deviceBlocksCount * blockSize, 1);
        TString zeroBlock(blockSize, 0);

        auto storage = std::make_shared<TTestStorage>();

        auto factory =
            CreateDeviceHandlerFactoryForTesting(blocksCountLimit * blockSize);
        auto deviceHandler = factory->CreateDeviceHandler(
            TDeviceHandlerParams{
                .Storage = storage,
                .DiskId = diskId,
                .ClientId = clientId,
                .BlockSize = blockSize,
                .MaxZeroBlocksSubRequestSize = maxZeroBlocksSubRequestSize,
                .UnalignedRequestsDisabled = unalignedRequestDisabled,
                .StorageMediaKind = NProto::STORAGE_MEDIA_SSD_NONREPLICATED});

        storage->ZeroBlocksHandler = [&] (
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TZeroBlocksRequest> request)
        {
            Y_UNUSED(callContext);

            UNIT_ASSERT(request->GetHeaders().GetClientId() == clientId);
            UNIT_ASSERT(request->GetBlocksCount() <= blocksCountLimit);
            UNIT_ASSERT(request->GetStartIndex() + request->GetBlocksCount() <= deviceBlocksCount);

            TSgList src(
                request->GetBlocksCount(),
                TBlockDataRef(zeroBlock.data(), zeroBlock.size()));

            TBlockDataRef dst(
                device.data() + request->GetStartIndex() * blockSize,
                src.size() * blockSize);

            auto bytesCount = SgListCopy(src, dst);
            UNIT_ASSERT_VALUES_EQUAL(dst.Size(), bytesCount);

            return MakeFuture(NProto::TZeroBlocksResponse());
        };

        storage->WriteBlocksLocalHandler = [&] (
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
        {
            Y_UNUSED(callContext);

            UNIT_ASSERT(request->GetHeaders().GetClientId() == clientId);
            UNIT_ASSERT(request->BlocksCount <= blocksCountLimit);
            UNIT_ASSERT(request->GetStartIndex() + request->BlocksCount <= deviceBlocksCount);

            TBlockDataRef dst(
                device.data() + request->GetStartIndex() * blockSize,
                request->BlocksCount * blockSize);

            auto guard = request->Sglist.Acquire();
            UNIT_ASSERT(guard);

            auto bytesCount = SgListCopy(guard.Get(), dst);
            UNIT_ASSERT_VALUES_EQUAL(dst.Size(), bytesCount);

            return MakeFuture(NProto::TWriteBlocksResponse());
        };

        storage->ReadBlocksLocalHandler = [&] (
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
        {
            Y_UNUSED(callContext);

            UNIT_ASSERT(request->GetHeaders().GetClientId() == clientId);
            UNIT_ASSERT(request->GetBlocksCount() <= blocksCountLimit);
            UNIT_ASSERT(request->GetStartIndex() + request->GetBlocksCount() <= deviceBlocksCount);

            TBlockDataRef src(
                device.data() + request->GetStartIndex() * blockSize,
                request->GetBlocksCount() * blockSize);

            NProto::TReadBlocksLocalResponse response;

            auto guard = request->Sglist.Acquire();
            UNIT_ASSERT(guard);

            auto bytesCount = SgListCopy(src, guard.Get());
            UNIT_ASSERT_VALUES_EQUAL(src.Size(), bytesCount);

            return MakeFuture(std::move(response));
        };

        ui64 from = requestUnaligned ? 4567 : 0;
        ui64 length = deviceBlocksCount * blockSize - (requestUnaligned ? 9876 : 0);

        auto future = deviceHandler->Zero(
            MakeIntrusive<TCallContext>(),
            from,
            length);

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(response), response);

        for (ui64 i = 0; i < deviceBlocksCount * blockSize; ++i) {
            bool isZero = (from <= i && i < (from + length));
            UNIT_ASSERT_VALUES_EQUAL_C(isZero ? 0 : 1, device[i], i);
        }
    }

    Y_UNIT_TEST(ShouldSliceHugeUnalignedZeroRequest) {
        DoShouldSliceHugeZeroRequest(true, false);
    }

    Y_UNIT_TEST(ShouldSliceHugeAlignedZeroRequest) {
        DoShouldSliceHugeZeroRequest(false, false);
    }

    Y_UNIT_TEST(ShouldSliceHugeZeroRequestWhenUnalignedDisabled) {
        DoShouldSliceHugeZeroRequest(false, true);
    }

    Y_UNIT_TEST(ShouldReturnErrorForHugeUnalignedReadWriteRequests)
    {
        const auto diskId = "disk1";
        const auto clientId = "testClientId";
        const ui32 blockSize = DefaultBlockSize;
        const ui32 maxZeroBlocksSubRequestSize = 0;

        auto storage = std::make_shared<TTestStorage>();

        auto deviceHandler =
            CreateDefaultDeviceHandlerFactory()->CreateDeviceHandler(
                TDeviceHandlerParams{
                    .Storage = storage,
                    .DiskId = diskId,
                    .ClientId = clientId,
                    .BlockSize = blockSize,
                    .MaxZeroBlocksSubRequestSize = maxZeroBlocksSubRequestSize,
                    .UnalignedRequestsDisabled = false,
                    .StorageMediaKind =
                        NProto::STORAGE_MEDIA_SSD_NONREPLICATED});

        storage->WriteBlocksLocalHandler = [&] (
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
        {
            Y_UNUSED(callContext);
            Y_UNUSED(request);
            return MakeFuture(NProto::TWriteBlocksResponse());
        };

        storage->ReadBlocksLocalHandler = [&] (
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
        {
            Y_UNUSED(callContext);
            Y_UNUSED(request);
            return MakeFuture(NProto::TReadBlocksLocalResponse());
        };

        ui64 from = 1;
        ui64 length = 64_MB;

        {
            TGuardedSgList sgList;
            TString checkpointId;
            auto future = deviceHandler->Read(
                MakeIntrusive<TCallContext>(),
                from,
                length,
                sgList,
                checkpointId);

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(HasError(response));
            UNIT_ASSERT(response.GetError().GetCode() == E_ARGUMENT);
        }

        {
            TGuardedSgList sgList;
            auto future = deviceHandler->Write(
                MakeIntrusive<TCallContext>(),
                from,
                length,
                sgList);

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(HasError(response));
            UNIT_ASSERT(response.GetError().GetCode() == E_ARGUMENT);
        }
    }

    Y_UNIT_TEST(ShouldReturnErrorForInvalidBufferSize)
    {
        const auto diskId = "disk1";
        const auto clientId = "testClientId";
        const ui32 blockSize = DefaultBlockSize;
        const ui32 maxZeroBlocksSubRequestSize = 0;

        auto storage = std::make_shared<TTestStorage>();

        auto deviceHandler =
            CreateDefaultDeviceHandlerFactory()->CreateDeviceHandler(
                TDeviceHandlerParams{
                    .Storage = storage,
                    .DiskId = diskId,
                    .ClientId = clientId,
                    .BlockSize = blockSize,
                    .MaxZeroBlocksSubRequestSize = maxZeroBlocksSubRequestSize,
                    .UnalignedRequestsDisabled = false,
                    .StorageMediaKind =
                        NProto::STORAGE_MEDIA_SSD_NONREPLICATED});

        storage->WriteBlocksLocalHandler = [&] (
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
        {
            Y_UNUSED(callContext);
            Y_UNUSED(request);
            return MakeFuture(NProto::TWriteBlocksResponse());
        };

        storage->ReadBlocksLocalHandler = [&] (
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
        {
            Y_UNUSED(callContext);
            Y_UNUSED(request);
            return MakeFuture(NProto::TReadBlocksLocalResponse());
        };

        ui64 from = 0;
        ui64 length = blockSize;
        auto buffer = TString::Uninitialized(blockSize + 1);
        TSgList sgList{{ buffer.data(), buffer.size() }};

        {
            TString checkpointId;
            auto future = deviceHandler->Read(
                MakeIntrusive<TCallContext>(),
                from,
                length,
                TGuardedSgList(sgList),
                checkpointId);

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(HasError(response));
            UNIT_ASSERT(response.GetError().GetCode() == E_ARGUMENT);
        }

        {
            auto future = deviceHandler->Write(
                MakeIntrusive<TCallContext>(),
                from,
                length,
                TGuardedSgList(sgList));

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(HasError(response));
            UNIT_ASSERT(response.GetError().GetCode() == E_ARGUMENT);
        }
    }

    Y_UNIT_TEST(ShouldSumPostponedTimeForReadModifyWriteRequests)
    {
        TTestEnvironment env(2, DefaultBlockSize, 8);

        auto ctx1 = env.WriteSectors(1, 2, 'a');
        auto ctx2 = env.WriteSectors(4, 4, 'b');
        auto ctx3 = env.ZeroSectors(5, 2);

        env.RunWriteService();

        UNIT_ASSERT_VALUES_EQUAL(ctx1->Time(EProcessingStage::Postponed), TDuration::Seconds(11));
        UNIT_ASSERT(ctx2->Time(EProcessingStage::Postponed) > TDuration::Seconds(11));
        UNIT_ASSERT(ctx3->Time(EProcessingStage::Postponed) > TDuration::Seconds(11));
    }

    Y_UNIT_TEST(ShouldNotOverflowStack)
    {
        // We are running a very long series of overlapping queries. If the
        // executed requests are not destroyed gradually, but do so after the
        // end of the cycle, stack overflow will occur. Therefore, let's choose
        // the number of iterations large enough so that stack overflow is
        // guaranteed to happen.
#if defined(_tsan_enabled_) || defined(_asan_enabled_)
        constexpr ui32 RequestCount = 1000;
#else
#if defined(NDEBUG)
        constexpr ui32 RequestCount = 1000000;
#else
        constexpr ui32 RequestCount = 100000;
#endif   // defined(NDEBUG)
#endif   // defined(_tsan_enabled_) || defined(_asan_enabled_)

        TTestEnvironment env(2, DefaultBlockSize, 8);

        // Create first request.
        env.WriteSectors(0, 4, 'a');

        for (ui32 i = 0; i < RequestCount; ++i) {
            // Take request trigger for previous request.
            auto [writeTrigger, futures] = env.TakeWriteTrigger();
            UNIT_ASSERT_VALUES_EQUAL(1, futures.size());

            // Create new request
            env.WriteSectors(i % 4, 4, 'b');

            // Execute previous request
            writeTrigger.SetValue();
            for (const auto& future: futures) {
                const auto& response = future.GetValue(TDuration::Seconds(5));
                UNIT_ASSERT(!HasError(response));
            }
        }

        // Execute last request
        env.RunWriteService();
    }

    Y_UNIT_TEST(ShouldReportCriticalEventOnError)
    {
        const auto diskId = "disk1";
        const auto clientId = "testClientId";
        const ui32 blockSize = DefaultBlockSize;
        const ui64 deviceBlocksCount = 8 * 1024;
        const ui64 blocksCountLimit = deviceBlocksCount / 4;
        const ui32 maxZeroBlocksSubRequestSize = 0;

        auto storage = std::make_shared<TTestStorage>();

        auto factory =
            CreateDeviceHandlerFactoryForTesting(blocksCountLimit * blockSize);
        auto deviceHandlerForReliableDisk = factory->CreateDeviceHandler(
            TDeviceHandlerParams{
                .Storage = storage,
                .DiskId = diskId,
                .ClientId = clientId,
                .BlockSize = blockSize,
                .MaxZeroBlocksSubRequestSize = maxZeroBlocksSubRequestSize,
                .UnalignedRequestsDisabled = false,
                .StorageMediaKind = NProto::STORAGE_MEDIA_SSD_MIRROR3});
        auto deviceHandlerForNonReliableDisk = factory->CreateDeviceHandler(
            TDeviceHandlerParams{
                .Storage = storage,
                .DiskId = diskId,
                .ClientId = clientId,
                .BlockSize = blockSize,
                .MaxZeroBlocksSubRequestSize = maxZeroBlocksSubRequestSize,
                .StorageMediaKind = NProto::STORAGE_MEDIA_SSD_NONREPLICATED});

        auto buffer = TString(DefaultBlockSize, 'g');
        TSgList sgList{{buffer.data(), buffer.size()}};
        storage->WriteBlocksLocalHandler =
            [&](TCallContextPtr ctx,
                std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
        {
            Y_UNUSED(ctx);
            Y_UNUSED(request);
            return MakeFuture<NProto::TWriteBlocksLocalResponse>(
                TErrorResponse{E_IO});
        };
        storage->ReadBlocksLocalHandler =
            [&](TCallContextPtr ctx,
                std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
        {
            Y_UNUSED(ctx);
            Y_UNUSED(request);
            return MakeFuture<NProto::TReadBlocksLocalResponse>(
                TErrorResponse{E_IO});
        };
        storage->ZeroBlocksHandler =
            [&](TCallContextPtr ctx,
                std::shared_ptr<NProto::TZeroBlocksRequest> request)
        {
            Y_UNUSED(ctx);
            Y_UNUSED(request);
            return MakeFuture<NProto::TZeroBlocksResponse>(
                TErrorResponse{E_IO});
        };
        auto counters = SetupCriticalEvents();
        auto reliableCritEvent = counters->GetCounter(
            "AppCriticalEvents/ErrorWasSentToTheGuestForReliableDisk",
            true);
        auto nonReliableCritEvent = counters->GetCounter(
            "AppCriticalEvents/ErrorWasSentToTheGuestForNonReliableDisk",
            true);

        {
            // Two write errors generate same critical events count for reliable
            // disk.
            reliableCritEvent->Set(0);
            nonReliableCritEvent->Set(0);
            {
                auto future = deviceHandlerForReliableDisk->Write(
                    MakeIntrusive<TCallContext>(),
                    0,
                    blockSize,
                    TGuardedSgList(sgList));
                UNIT_ASSERT(HasError(future.GetValue(TDuration::Seconds(5))));
            }
            {
                auto future = deviceHandlerForReliableDisk->Write(
                    MakeIntrusive<TCallContext>(),
                    0,
                    blockSize,
                    TGuardedSgList(sgList));
                UNIT_ASSERT(HasError(future.GetValue(TDuration::Seconds(5))));
            }
            UNIT_ASSERT_VALUES_EQUAL(2, reliableCritEvent->Val());
            UNIT_ASSERT_VALUES_EQUAL(0, nonReliableCritEvent->Val());
        }

        {
            // Two write errors generate only one critical event for non
            // reliable disk.
            reliableCritEvent->Set(0);
            nonReliableCritEvent->Set(0);
            {
                auto future = deviceHandlerForNonReliableDisk->Write(
                    MakeIntrusive<TCallContext>(),
                    0,
                    blockSize,
                    TGuardedSgList(sgList));
                UNIT_ASSERT(HasError(future.GetValue(TDuration::Seconds(5))));
            }
            {
                auto future = deviceHandlerForNonReliableDisk->Write(
                    MakeIntrusive<TCallContext>(),
                    0,
                    blockSize,
                    TGuardedSgList(sgList));
                UNIT_ASSERT(HasError(future.GetValue(TDuration::Seconds(5))));
            }
            UNIT_ASSERT_VALUES_EQUAL(0, reliableCritEvent->Val());
            UNIT_ASSERT_VALUES_EQUAL(1, nonReliableCritEvent->Val());
        }

        {
            // Check critical event for read.
            reliableCritEvent->Set(0);
            nonReliableCritEvent->Set(0);

            auto buffer = TString::Uninitialized(DefaultBlockSize);
            TSgList sgList{{buffer.data(), buffer.size()}};

            auto future = deviceHandlerForReliableDisk->Read(
                MakeIntrusive<TCallContext>(),
                0,
                DefaultBlockSize,
                TGuardedSgList(sgList),
                {});
            UNIT_ASSERT(HasError(future.GetValue(TDuration::Seconds(5))));
            Cout << FormatError(future.GetValue().GetError());

            UNIT_ASSERT_VALUES_EQUAL(1, reliableCritEvent->Val());
            UNIT_ASSERT_VALUES_EQUAL(0, nonReliableCritEvent->Val());
        }

        {
            // Check critical event for zero.
            reliableCritEvent->Set(0);
            nonReliableCritEvent->Set(0);

            auto future = deviceHandlerForReliableDisk->Zero(
                MakeIntrusive<TCallContext>(),
                0,
                1);
            UNIT_ASSERT(HasError(future.GetValue(TDuration::Seconds(5))));
            UNIT_ASSERT(HasError(future.GetValue(TDuration::Seconds(5))));
            UNIT_ASSERT_VALUES_EQUAL(1, reliableCritEvent->Val());
            UNIT_ASSERT_VALUES_EQUAL(0, nonReliableCritEvent->Val());
        }
    }

    Y_UNIT_TEST(ShouldNotReportCriticalEventOnReadOnlyMountViolation)
    {
        const auto diskId = "disk1";
        const auto clientId = "testClientId";
        const ui32 blockSize = DefaultBlockSize;
        const ui64 deviceBlocksCount = 8 * 1024;
        const ui64 blocksCountLimit = deviceBlocksCount / 4;
        const ui32 maxZeroBlocksSubRequestSize = 0;

        auto storage = std::make_shared<TTestStorage>();

        auto factory =
            CreateDeviceHandlerFactoryForTesting(blocksCountLimit * blockSize);
        auto deviceHandlerForReliableDisk = factory->CreateDeviceHandler(
            TDeviceHandlerParams{
                .Storage = storage,
                .DiskId = diskId,
                .ClientId = clientId,
                .BlockSize = blockSize,
                .MaxZeroBlocksSubRequestSize = maxZeroBlocksSubRequestSize,
                .UnalignedRequestsDisabled = false,
                .StorageMediaKind = NProto::STORAGE_MEDIA_SSD});
        auto deviceHandlerForNonReliableDisk = factory->CreateDeviceHandler(
            TDeviceHandlerParams{
                .Storage = storage,
                .DiskId = diskId,
                .ClientId = clientId,
                .BlockSize = blockSize,
                .MaxZeroBlocksSubRequestSize = maxZeroBlocksSubRequestSize,
                .UnalignedRequestsDisabled = false,
                .StorageMediaKind = NProto::STORAGE_MEDIA_SSD_NONREPLICATED});

        auto buffer = TString(DefaultBlockSize, 'g');
        TSgList sgList{{buffer.data(), buffer.size()}};
        storage->WriteBlocksLocalHandler =
            [&](TCallContextPtr ctx,
                std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
        {
            Y_UNUSED(ctx);
            Y_UNUSED(request);
            return MakeFuture<NProto::TWriteBlocksLocalResponse>(TErrorResponse{
                E_IO_SILENT,
                R"(Request WriteBlocks is not allowed for client "xxxx" and volume "yyyyy")"});
        };

        storage->ZeroBlocksHandler =
            [&](TCallContextPtr ctx,
                std::shared_ptr<NProto::TZeroBlocksRequest> request)
        {
            Y_UNUSED(ctx);
            Y_UNUSED(request);
            return MakeFuture<NProto::TZeroBlocksResponse>(TErrorResponse{
                E_IO_SILENT,
                R"(Request ZeroBlocks is not allowed for client "xxxx" and volume "yyyyy")"});
        };

        auto counters = SetupCriticalEvents();
        auto reliableCritEvent = counters->GetCounter(
            "AppCriticalEvents/ErrorWasSentToTheGuestForReliableDisk",
            true);
        auto nonReliableCritEvent = counters->GetCounter(
            "AppCriticalEvents/ErrorWasSentToTheGuestForNonReliableDisk",
            true);

        reliableCritEvent->Set(0);
        nonReliableCritEvent->Set(0);
        {
            auto future = deviceHandlerForReliableDisk->Write(
                MakeIntrusive<TCallContext>(),
                0,
                blockSize,
                TGuardedSgList(sgList));
            UNIT_ASSERT(HasError(future.GetValue(TDuration::Seconds(5))));

            future = deviceHandlerForNonReliableDisk->Write(
                MakeIntrusive<TCallContext>(),
                0,
                blockSize,
                TGuardedSgList(sgList));
            UNIT_ASSERT(HasError(future.GetValue(TDuration::Seconds(5))));
        }

        {
            auto future = deviceHandlerForReliableDisk->Zero(
                MakeIntrusive<TCallContext>(),
                0,
                blockSize);
            UNIT_ASSERT(HasError(future.GetValue(TDuration::Seconds(5))));

            future = deviceHandlerForNonReliableDisk->Zero(
                MakeIntrusive<TCallContext>(),
                0,
                blockSize);
            UNIT_ASSERT(HasError(future.GetValue(TDuration::Seconds(5))));
        }

        UNIT_ASSERT_VALUES_EQUAL(0, reliableCritEvent->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, nonReliableCritEvent->Val());
    }
}

}   // namespace NCloud::NBlockStore
