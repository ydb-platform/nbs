#include "server_handler.h"

#include "error_handler.h"
#include "protocol.h"
#include "utils.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/diagnostics/server_stats_test.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/device_handler.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/storage_test.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/io_depth_tracker.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>
#include <util/stream/str.h>
#include <util/system/event.h>

#include <array>
#include <atomic>
#include <functional>
#include <thread>

namespace NCloud::NBlockStore::NBD {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

static const TString DefaultDiskId = "test";
static const ui64 DefaultBlocksCount = 1024;

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
private:
    ILoggingServicePtr Logging;
    IStoragePtr Storage;

public:
    TBootstrap(
            ILoggingServicePtr logging,
            IStoragePtr storage)
        : Logging(std::move(logging))
        , Storage(std::move(storage))
    {}

    void Start()
    {
        if (Logging) {
            Logging->Start();
        }
    }

    void Stop()
    {
        if (Logging) {
            Logging->Stop();
        }
    }

    ILoggingServicePtr GetLogging()
    {
        return Logging;
    }

    IStoragePtr GetStorage()
    {
        return Storage;
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TBootstrap> CreateBootstrap(
    std::shared_ptr<TTestStorage> storage)
{
    return std::make_unique<TBootstrap>(
        CreateLoggingService("console", { TLOG_DEBUG }),
        std::move(storage));
}

////////////////////////////////////////////////////////////////////////////////

class TServerContext
    : public IServerContext
{
private:
    IOutputStream& Out;

public:
    IServerHandler* Handler = nullptr;
    bool Deferred = false;
    bool ForwardResponses = true;
    TVector<ITaskPtr> PendingTasks;
    TServerResponsePtr LastResponse;
    std::function<void()> BeforeWait;

    TServerContext(IOutputStream& out)
        : Out(out)
    {}

    void ExecutePending()
    {
        auto tasks = std::move(PendingTasks);
        for (auto& task: tasks) {
            task->Execute();
        }
    }

    void Start() override
    {
    }

    void Stop() override
    {
    }

    bool AcquireRequest(size_t requestBytes) override
    {
        Y_UNUSED(requestBytes);
        return true;
    }

    void Enqueue(ITaskPtr task) override
    {
        if (Deferred) {
            PendingTasks.push_back(std::move(task));
        } else {
            task->Execute();
        }
    }

    const NProto::TReadBlocksLocalResponse& WaitFor(
        const TFuture<NProto::TReadBlocksLocalResponse>& future) override
    {
        if (BeforeWait) {
            BeforeWait();
        }
        return future.GetValue(TDuration::Max());
    }

    const NProto::TWriteBlocksLocalResponse& WaitFor(
        const TFuture<NProto::TWriteBlocksLocalResponse>& future) override
    {
        if (BeforeWait) {
            BeforeWait();
        }
        return future.GetValue(TDuration::Max());
    }

    const NProto::TZeroBlocksResponse& WaitFor(
        const TFuture<NProto::TZeroBlocksResponse>& future) override
    {
        if (BeforeWait) {
            BeforeWait();
        }
        return future.GetValue(TDuration::Max());
    }

    void SendResponse(TServerResponsePtr response) override
    {
        LastResponse = response;
        if (Handler) {
            if (ForwardResponses) {
                Handler->SendResponse(Out, *response);
            }
            return;
        }

        Out.Write(response->HeaderBuffer.Data(), response->HeaderBuffer.Size());

        if (response->DataBuffer) {
            Out.Write(response->DataBuffer.get(), response->RequestBytes);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

void SetupStorage(TTestStorage& storage)
{
    storage.ZeroBlocksHandler = [&] (
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request)
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);
        return MakeFuture<NProto::TZeroBlocksResponse>();
    };

    storage.ReadBlocksLocalHandler = [&] (
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);
        return MakeFuture<NProto::TReadBlocksLocalResponse>();
    };

    storage.WriteBlocksLocalHandler = [&] (
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);
        return MakeFuture<NProto::TWriteBlocksLocalResponse>();
    };
}

TExportInfo NegotiateClient(
    IServerHandler& handler,
    TStringStream& in,
    TStringStream& out)
{
    TRequestReader reader(in);
    TRequestWriter writer(out);

    writer.WriteClientHello(NBD_FLAG_C_FIXED_NEWSTYLE | NBD_FLAG_C_NO_ZEROES);
    writer.WriteOption(NBD_OPT_STRUCTURED_REPLY);
    writer.WriteOption(NBD_OPT_LIST);

    {
        TExportInfoRequest request;
        request.InfoTypes = { NBD_INFO_NAME, NBD_INFO_BLOCK_SIZE };

        TBufferRequestWriter requestOut;
        requestOut.WriteExportInfoRequest(request);

        writer.WriteOption(NBD_OPT_GO, AsStringBuf(requestOut.Buffer()));
    }

    UNIT_ASSERT(handler.NegotiateClient(out, in));

    TExportInfo result {};

    {
        TServerHello hello;
        UNIT_ASSERT(reader.ReadServerHello(hello));
        UNIT_ASSERT(hello.Flags == NBD_FLAG_FIXED_NEWSTYLE | NBD_FLAG_NO_ZEROES);
    }

    {
        TOptionReply reply;
        TBuffer replyData;
        UNIT_ASSERT(reader.ReadOptionReply(reply, replyData));
        UNIT_ASSERT(reply.Option == NBD_OPT_STRUCTURED_REPLY);
        UNIT_ASSERT(reply.Type == NBD_REP_ACK);
        UNIT_ASSERT(!replyData.Size());
    }

    {
        TOptionReply reply;
        TBuffer replyData;
        UNIT_ASSERT(reader.ReadOptionReply(reply, replyData));
        UNIT_ASSERT(reply.Option == NBD_OPT_LIST);
        UNIT_ASSERT(reply.Type == NBD_REP_SERVER);
        TBufferRequestReader response(replyData);

        TExportInfo exp {};
        UNIT_ASSERT(response.ReadExportList(exp));
        UNIT_ASSERT(exp.Name == DefaultDiskId);
    }

    {
        TOptionReply reply;
        TBuffer replyData;
        UNIT_ASSERT(reader.ReadOptionReply(reply, replyData));
        UNIT_ASSERT(reply.Option == NBD_OPT_LIST);
        UNIT_ASSERT(reply.Type == NBD_REP_ACK);
        UNIT_ASSERT(!replyData.Size());
    }

    {
        TOptionReply reply;
        TBuffer replyData;
        UNIT_ASSERT(reader.ReadOptionReply(reply, replyData));
        UNIT_ASSERT(reply.Option == NBD_OPT_GO);
        UNIT_ASSERT(reply.Type == NBD_REP_INFO);
        TBufferRequestReader replyIn(replyData);

        TExportInfo exp {};
        ui16 type;
        UNIT_ASSERT(replyIn.ReadExportInfo(exp, type));
        UNIT_ASSERT(type == NBD_INFO_NAME);
        UNIT_ASSERT(exp.Name == DefaultDiskId);
        result.Name = exp.Name;
    }

    {
        TOptionReply reply;
        TBuffer replyData;
        UNIT_ASSERT(reader.ReadOptionReply(reply, replyData));
        UNIT_ASSERT(reply.Option == NBD_OPT_GO);
        UNIT_ASSERT(reply.Type == NBD_REP_INFO);
        TBufferRequestReader replyIn(replyData);

        TExportInfo exp {};
        ui16 type;
        UNIT_ASSERT(replyIn.ReadExportInfo(exp, type));
        UNIT_ASSERT(type == NBD_INFO_BLOCK_SIZE);
        result.MinBlockSize = exp.MinBlockSize;
        result.OptBlockSize = exp.OptBlockSize;
        result.MaxBlockSize = exp.MaxBlockSize;
    }

    {
        TOptionReply reply;
        TBuffer replyData;
        UNIT_ASSERT(reader.ReadOptionReply(reply, replyData));
        UNIT_ASSERT(reply.Option == NBD_OPT_GO);
        UNIT_ASSERT(reply.Type == NBD_REP_INFO);
        TBufferRequestReader replyIn(replyData);

        TExportInfo exp {};
        ui16 type;
        UNIT_ASSERT(replyIn.ReadExportInfo(exp, type));
        UNIT_ASSERT(type == NBD_INFO_EXPORT);
        result.Size = exp.Size;
        result.Flags = exp.Flags;
    }

    {
        TOptionReply reply;
        TBuffer replyData;
        UNIT_ASSERT(reader.ReadOptionReply(reply, replyData));
        UNIT_ASSERT(reply.Option == NBD_OPT_GO);
        UNIT_ASSERT(reply.Type == NBD_REP_ACK);
        UNIT_ASSERT(!replyData.Size());
    }

    return result;
}

class TIoDepthHandlerFixture
{
public:
    ui64 NowNs = 0;
    TIoDepthTracker Depth{
        BlockStoreRequestsCount,
        [this]
        {
            return NowNs;
        }};
    ui32 CompletedCount = 0;
    ui32 ErrorCount = 0;
    bool Balanced = true;
    std::shared_ptr<TTestStorage> Storage;
    std::shared_ptr<TTestServerStats> Stats =
        std::make_shared<TTestServerStats>();
    IServerHandlerPtr Handler;
    TStringStream Replies;
    TStringStream Requests;
    TIntrusivePtr<TServerContext> Context;

    explicit TIoDepthHandlerFixture(
        std::shared_ptr<TTestStorage> storage =
            std::make_shared<TTestStorage>())
        : Storage(std::move(storage))
    {
        Storage->DoAllocations = true;
        SetupStorage(*Storage);
        Stats->RequestStartedHandler =
            [this](
                TLog&, TMetricRequest& request, TCallContext&, const TString&)
        {
            if (IsNonLocalReadWriteRequest(request.RequestType)) {
                Depth.Started(static_cast<ui32>(request.RequestType));
            }
        };
        Stats->RequestCompletedHandler = [this](
                                             TLog&,
                                             TMetricRequest& request,
                                             TCallContext&,
                                             const NProto::TError& error)
        {
            if (IsNonLocalReadWriteRequest(request.RequestType)) {
                Balanced &=
                    Depth.Completed(static_cast<ui32>(request.RequestType));
                ++CompletedCount;
                ErrorCount += HasError(error);
            }
        };
        TStorageOptions options;
        options.DiskId = DefaultDiskId;
        options.BlockSize = DefaultBlockSize;
        options.BlocksCount = DefaultBlocksCount;
        Handler = CreateServerHandlerFactory(
                      CreateDefaultDeviceHandlerFactory(),
                      CreateLoggingService("console", {TLOG_DEBUG}),
                      Storage, Stats, CreateErrorHandlerStub(), options)
                      ->CreateHandler();
        NegotiateClient(*Handler, Replies, Requests);
        Context = MakeIntrusive<TServerContext>(Replies);
        Context->Handler = Handler.get();
        Context->Deferred = true;
    }

    void Accept(ui32 command, bool completePayload = true)
    {
        TRequest request{};
        request.Magic = NBD_REQUEST_MAGIC;
        request.Type = command;
        request.Handle = 1;
        request.Length = DefaultBlockSize;
        TRequestWriter writer(Requests);
        if (command == NBD_CMD_WRITE) {
            if (completePayload) {
                writer.WriteRequest(request, TString(request.Length, 'a'));
            } else {
                TStringStream encoded;
                TRequestWriter encodedWriter(encoded);
                encodedWriter.WriteRequest(
                    request, TString(request.Length, 'a'));
                const auto& bytes = encoded.Str();
                Requests.Write(bytes.data(), bytes.size() - request.Length);
            }
        } else {
            writer.WriteRequest(request);
        }
        Handler->ProcessRequests(Context, Requests, Replies, nullptr);
    }
};

class TThrowingIoDepthOutput final: public IOutputStream
{
    void DoWrite(const void*, size_t) override
    {
        ythrow yexception() << "response write failed";
    }
};

class TBufferLifetimeStorage final: public TTestStorage
{
public:
    std::atomic<bool> GuardHeld = false;
    std::atomic<bool> ReleasedWithGuard = false;
    std::atomic<bool> BufferReleased = false;

    TStorageBuffer AllocateBuffer(size_t bytes) override
    {
        return std::shared_ptr<char>(
            new char[bytes],
            [this](char* buffer)
            {
                ReleasedWithGuard = GuardHeld.load();
                BufferReleased = true;
                delete[] buffer;
            });
    }
};

////////////////////////////////////////////////////////////////////////////////

void ProcessRequests(
    IServerHandler& handler,
    TStringStream& in,
    TStringStream& out,
    ui32 length = 4*1024)
{
    TRequestReader reader(in);
    TRequestWriter writer(out);

    {
        TRequest request;
        request.Magic = NBD_REQUEST_MAGIC;
        request.Flags = 0;
        request.Type = NBD_CMD_READ;
        request.Handle = 1;
        request.From = 0;
        request.Length = length;

        writer.WriteRequest(request);
    }

    {
        TRequest request;
        request.Magic = NBD_REQUEST_MAGIC;
        request.Flags = 0;
        request.Type = NBD_CMD_WRITE;
        request.Handle = 2;
        request.From = 0;
        request.Length = length;

        writer.WriteRequest(request, TString(request.Length, 'a'));
    }

    {
        TRequest request;
        request.Magic = NBD_REQUEST_MAGIC;
        request.Flags = 0;
        request.Type = NBD_CMD_WRITE_ZEROES;
        request.Handle = 3;
        request.From = 0;
        request.Length = length;

        writer.WriteRequest(request);
    }

    auto ctx = MakeIntrusive<TServerContext>(in);
    handler.ProcessRequests(ctx, out, in, nullptr);

    {
        TStructuredReply reply;
        TBuffer replyData;
        UNIT_ASSERT(reader.ReadStructuredReply(reply));
        reader.ReadStructuredReplyData(reply, replyData);
        UNIT_ASSERT(reply.Type == NBD_REPLY_TYPE_OFFSET_DATA);
        UNIT_ASSERT(reply.Handle == 1);
        UNIT_ASSERT(replyData.Size() == length);
    }

    {
        TStructuredReply reply;
        TBuffer replyData;
        UNIT_ASSERT(reader.ReadStructuredReply(reply));
        reader.ReadStructuredReplyData(reply, replyData);
        UNIT_ASSERT(reply.Type == NBD_REPLY_TYPE_NONE);
        UNIT_ASSERT(reply.Handle == 2);
        UNIT_ASSERT(!replyData.Size());
    }

    {
        TStructuredReply reply;
        TBuffer replyData;
        UNIT_ASSERT(reader.ReadStructuredReply(reply));
        reader.ReadStructuredReplyData(reply, replyData);
        UNIT_ASSERT(reply.Type == NBD_REPLY_TYPE_NONE);
        UNIT_ASSERT(reply.Handle == 3);
        UNIT_ASSERT(!replyData.Size());
    }
}

void ProcessUnalignedRequests(
    IServerHandler& handler,
    TStringStream& in,
    TStringStream& out)
{
    TRequestReader reader(in);
    TRequestWriter writer(out);

    {
        TRequest request;
        request.Magic = NBD_REQUEST_MAGIC;
        request.Flags = 0;
        request.Type = NBD_CMD_READ;
        request.Handle = 1;
        request.From = 13 * 512;
        request.Length = 11 * 512;

        writer.WriteRequest(request);
    }

    {
        TRequest request;
        request.Magic = NBD_REQUEST_MAGIC;
        request.Flags = 0;
        request.Type = NBD_CMD_WRITE;
        request.Handle = 2;
        request.From = 8 * 512;
        request.Length = 13 * 512;

        writer.WriteRequest(request, TString(request.Length, 'a'));
    }

    {
        TRequest request;
        request.Magic = NBD_REQUEST_MAGIC;
        request.Flags = 0;
        request.Type = NBD_CMD_WRITE_ZEROES;
        request.Handle = 3;
        request.From = 13 * 512;
        request.Length = 8 * 512;

        writer.WriteRequest(request);
    }

    auto ctx = MakeIntrusive<TServerContext>(in);
    handler.ProcessRequests(ctx, out, in, nullptr);

    {
        TStructuredReply reply;
        TBuffer replyData;
        UNIT_ASSERT(reader.ReadStructuredReply(reply));
        reader.ReadStructuredReplyData(reply, replyData);
        UNIT_ASSERT(reply.Type == NBD_REPLY_TYPE_OFFSET_DATA);
        UNIT_ASSERT(reply.Handle == 1);
        UNIT_ASSERT(replyData.Size() == 11 * 512);
    }

    {
        TStructuredReply reply;
        TBuffer replyData;
        UNIT_ASSERT(reader.ReadStructuredReply(reply));
        reader.ReadStructuredReplyData(reply, replyData);
        UNIT_ASSERT(reply.Type == NBD_REPLY_TYPE_NONE);
        UNIT_ASSERT(reply.Handle == 2);
        UNIT_ASSERT(!replyData.Size());
    }

    {
        TStructuredReply reply;
        TBuffer replyData;
        UNIT_ASSERT(reader.ReadStructuredReply(reply));
        reader.ReadStructuredReplyData(reply, replyData);
        UNIT_ASSERT(reply.Type == NBD_REPLY_TYPE_NONE);
        UNIT_ASSERT(reply.Handle == 3);
        UNIT_ASSERT(!replyData.Size());
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServerHandlerTest)
{
    Y_UNIT_TEST(ShouldAccumulateIoDepthWhileRequestTaskIsPending)
    {
        for (const auto& command:
             {std::pair{NBD_CMD_READ, EBlockStoreRequest::ReadBlocks},
              std::pair{NBD_CMD_WRITE, EBlockStoreRequest::WriteBlocks},
              std::pair{NBD_CMD_WRITE_ZEROES, EBlockStoreRequest::ZeroBlocks}})
        {
            TIoDepthHandlerFixture fixture;
            fixture.Accept(command.first);
            fixture.NowNs = 60'000'000'000ULL;
            const auto lane = static_cast<ui32>(command.second);
            const auto pending = fixture.Depth.Snapshot();
            UNIT_ASSERT_VALUES_EQUAL(pending.Lanes[lane].Current, 1);
            UNIT_ASSERT_VALUES_EQUAL(
                pending.Lanes[lane].IntegralUs, 60'000'000);
            UNIT_ASSERT_VALUES_EQUAL(fixture.CompletedCount, 0);

            fixture.Context->ExecutePending();
            const auto completed = fixture.Depth.Snapshot();
            UNIT_ASSERT(completed.Continuous);
            UNIT_ASSERT_VALUES_EQUAL(completed.Lanes[lane].Current, 0);
            UNIT_ASSERT_VALUES_EQUAL(
                completed.Lanes[lane].IntegralUs, 60'000'000);
            UNIT_ASSERT_VALUES_EQUAL(fixture.CompletedCount, 1);
        }
    }

    Y_UNIT_TEST(ShouldCompleteIoDepthForStorageFailureAndException)
    {
        for (const ui32 failure: {0, 1, 2}) {
            TIoDepthHandlerFixture fixture;
            fixture.Storage->ReadBlocksLocalHandler = [failure](auto, auto)
                -> TFuture<NProto::TReadBlocksLocalResponse>
            {
                if (failure == 1) {
                    ythrow yexception() << "storage read failed";
                }
                if (failure == 2) {
                    auto promise =
                        NewPromise<NProto::TReadBlocksLocalResponse>();
                    promise.SetException(std::make_exception_ptr(
                        yexception() << "future failed"));
                    return promise.GetFuture();
                }
                NProto::TReadBlocksLocalResponse response;
                *response.MutableError() = MakeError(E_IO, "read failure");
                return MakeFuture(response);
            };
            fixture.Accept(NBD_CMD_READ);
            fixture.NowNs = 1'000'000'000;
            fixture.Context->ExecutePending();
            const auto snapshot = fixture.Depth.Snapshot();
            const auto lane = static_cast<ui32>(EBlockStoreRequest::ReadBlocks);
            UNIT_ASSERT(snapshot.Continuous);
            UNIT_ASSERT_VALUES_EQUAL(snapshot.Lanes[lane].Current, 0);
            UNIT_ASSERT_VALUES_EQUAL(
                snapshot.Lanes[lane].IntegralUs, 1'000'000);
            UNIT_ASSERT_VALUES_EQUAL(fixture.CompletedCount, 1);
            UNIT_ASSERT_VALUES_EQUAL(fixture.ErrorCount, 1);
        }
    }

    Y_UNIT_TEST(ShouldCompleteIoDepthWhenResponseWriteThrows)
    {
        TIoDepthHandlerFixture fixture;
        fixture.Context->ForwardResponses = false;
        fixture.Accept(NBD_CMD_READ);
        fixture.NowNs = 1'000'000'000;
        fixture.Context->ExecutePending();
        UNIT_ASSERT(fixture.Context->LastResponse);
        UNIT_ASSERT_VALUES_EQUAL(fixture.CompletedCount, 0);

        TThrowingIoDepthOutput output;
        UNIT_ASSERT_EXCEPTION(
            fixture.Handler->SendResponse(
                output, *fixture.Context->LastResponse), yexception);
        const auto snapshot = fixture.Depth.Snapshot();
        UNIT_ASSERT(fixture.Balanced);
        const auto lane = static_cast<ui32>(EBlockStoreRequest::ReadBlocks);
        UNIT_ASSERT(snapshot.Continuous);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Lanes[lane].Current, 0);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Lanes[lane].IntegralUs, 1'000'000);
        UNIT_ASSERT_VALUES_EQUAL(fixture.CompletedCount, 1);
        fixture.Handler->CompleteResponse(*fixture.Context->LastResponse);
        UNIT_ASSERT_VALUES_EQUAL(fixture.CompletedCount, 1);
        UNIT_ASSERT(fixture.Depth.Snapshot().Continuous);
    }

    Y_UNIT_TEST(ShouldCloseStorageBuffersOnSuccessAndException)
    {
        for (const ui32 command: {NBD_CMD_READ, NBD_CMD_WRITE}) {
            // Successful response, synchronous throw, already exceptional
            // future, and an exception after all subscribers were installed.
            for (const ui32 failure: {0, 1, 2, 3}) {
                TIoDepthHandlerFixture fixture;
                TGuardedSgList retainedSgList;
                auto read = NewPromise<NProto::TReadBlocksLocalResponse>();
                auto write = NewPromise<NProto::TWriteBlocksLocalResponse>();
                const auto exception = std::make_exception_ptr(
                    yexception() << "storage buffer failure");
                auto complete = [&](const auto& request, auto& promise)
                {
                    retainedSgList = request->Sglist;
                    if (failure == 1) {
                        std::rethrow_exception(exception);
                    }
                    if (failure == 2) {
                        promise.SetException(exception);
                    } else if (failure == 0) {
                        promise.SetValue({});
                    }
                    return promise.GetFuture();
                };
                fixture.Storage->ReadBlocksLocalHandler =
                    [&](auto, auto request)
                {
                    return complete(request, read);
                };
                fixture.Storage->WriteBlocksLocalHandler =
                    [&](auto, auto request)
                {
                    return complete(request, write);
                };
                if (failure == 3) {
                    fixture.Context->BeforeWait = [&]
                    {
                        if (command == NBD_CMD_READ) {
                            read.SetException(exception);
                        } else {
                            write.SetException(exception);
                        }
                    };
                }

                fixture.Accept(command);
                fixture.NowNs = 1'000'000'000;
                fixture.Context->ExecutePending();

                // Storage retained a copy before returning/throwing. It must
                // lose access before the owner releases or sends the buffer.
                UNIT_ASSERT(!retainedSgList.Empty());
                UNIT_ASSERT(!retainedSgList.Acquire());
                UNIT_ASSERT(fixture.Balanced);
                UNIT_ASSERT_VALUES_EQUAL(fixture.CompletedCount, 1);
                UNIT_ASSERT_VALUES_EQUAL(fixture.ErrorCount, failure != 0);
                const auto lane = static_cast<ui32>(
                    command == NBD_CMD_READ ? EBlockStoreRequest::ReadBlocks
                                            : EBlockStoreRequest::WriteBlocks);
                const auto snapshot = fixture.Depth.Snapshot();
                UNIT_ASSERT(snapshot.Continuous);
                UNIT_ASSERT_VALUES_EQUAL(snapshot.Lanes[lane].Current, 0);
                UNIT_ASSERT_VALUES_EQUAL(snapshot.Lanes[lane].IntegralUs,
                                         1'000'000);
            }
        }
    }

    Y_UNIT_TEST(ShouldKeepFailedBuffersUntilStorageReleasesGuard)
    {
        for (const ui32 command: {NBD_CMD_READ, NBD_CMD_WRITE}) {
            auto storage = std::make_shared<TBufferLifetimeStorage>();
            TIoDepthHandlerFixture fixture(storage);
            TGuardedSgList retainedSgList;
            TManualEvent guardAcquired;
            TManualEvent completeRequest;
            TManualEvent releaseGuard;
            auto read = NewPromise<NProto::TReadBlocksLocalResponse>();
            auto write = NewPromise<NProto::TWriteBlocksLocalResponse>();
            std::atomic<bool> subscriberThrew = false;
            std::thread worker;
            Y_DEFER
            {
                completeRequest.Signal();
                releaseGuard.Signal();
                if (worker.joinable()) {
                    worker.join();
                }
            };
            auto launch = [&](auto request, const auto& response)
            {
                retainedSgList = request->Sglist;
                worker = std::thread(
                    [&, request, promise = response]() mutable
                    {
                        auto guard = request->Sglist.Acquire();
                        storage->GuardHeld = bool(guard);
                        guardAcquired.Signal();
                        completeRequest.WaitT(TDuration::Seconds(5));
                        try {
                            promise.SetException(std::make_exception_ptr(
                                yexception()
                                << "storage failed with an active guard"));
                        } catch (...) {
                            subscriberThrew = true;
                        }
                        // Keep the guard across exceptional completion. Close
                        // must wait before the owner can release the read/write
                        // buffer.
                        releaseGuard.WaitT(TDuration::MilliSeconds(100));
                        storage->GuardHeld = false;
                    });
                UNIT_ASSERT(guardAcquired.WaitT(TDuration::Seconds(5)));
                UNIT_ASSERT(storage->GuardHeld.load());
                return response.GetFuture();
            };
            storage->ReadBlocksLocalHandler = [&](auto, auto request)
            {
                return launch(request, read);
            };
            storage->WriteBlocksLocalHandler = [&](auto, auto request)
            {
                return launch(request, write);
            };
            fixture.Context->BeforeWait = [&]
            {
                completeRequest.Signal();
            };
            fixture.Accept(command);
            fixture.Context->ExecutePending();
            worker.join();

            UNIT_ASSERT(!subscriberThrew.load());
            UNIT_ASSERT(storage->BufferReleased.load());
            UNIT_ASSERT(!storage->ReleasedWithGuard.load());
            UNIT_ASSERT(!retainedSgList.Acquire());
            UNIT_ASSERT(fixture.Balanced);
            UNIT_ASSERT_VALUES_EQUAL(fixture.CompletedCount, 1);
            UNIT_ASSERT_VALUES_EQUAL(fixture.ErrorCount, 1);
        }
    }

    Y_UNIT_TEST(ShouldCompleteIoDepthForTruncatedWritePayload)
    {
        TIoDepthHandlerFixture fixture;
        UNIT_ASSERT_EXCEPTION(fixture.Accept(NBD_CMD_WRITE, false), yexception);
        fixture.NowNs = 1'000'000'000;
        fixture.Context->ExecutePending();
        const auto snapshot = fixture.Depth.Snapshot();
        UNIT_ASSERT(fixture.Balanced);
        const auto lane = static_cast<ui32>(EBlockStoreRequest::WriteBlocks);
        UNIT_ASSERT(snapshot.Continuous);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Lanes[lane].Current, 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.CompletedCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.ErrorCount, 1);
    }

    Y_UNIT_TEST(ShouldCancelIoDepthOnceBeforeLateTaskCompletion)
    {
        TIoDepthHandlerFixture fixture;
        fixture.Accept(NBD_CMD_READ);
        fixture.NowNs = 1'000'000'000;
        fixture.Handler->ProcessException(
            std::make_exception_ptr(yexception() << "connection closed"));
        const auto lane = static_cast<ui32>(EBlockStoreRequest::ReadBlocks);
        const auto cancelled = fixture.Depth.Snapshot();
        UNIT_ASSERT_VALUES_EQUAL(cancelled.Lanes[lane].Current, 0);
        UNIT_ASSERT_VALUES_EQUAL(cancelled.Lanes[lane].IntegralUs, 1'000'000);
        UNIT_ASSERT_VALUES_EQUAL(fixture.CompletedCount, 1);

        fixture.NowNs = 2'000'000'000;
        fixture.Context->ExecutePending();
        UNIT_ASSERT_VALUES_EQUAL(fixture.CompletedCount, 1);
        UNIT_ASSERT(fixture.Depth.Snapshot().Continuous);
        UNIT_ASSERT_VALUES_EQUAL(
            fixture.Depth.Snapshot().Lanes[lane].Current, 0);
    }

    Y_UNIT_TEST(ShouldNegotiateClient)
    {
        auto storage = std::make_shared<TTestStorage>();
        SetupStorage(*storage);

        auto bootstrap = CreateBootstrap(storage);
        bootstrap->Start();

        TStorageOptions options;
        options.DiskId = DefaultDiskId;
        options.BlockSize = DefaultBlockSize;
        options.BlocksCount = DefaultBlocksCount;

        auto factory = CreateServerHandlerFactory(
            CreateDefaultDeviceHandlerFactory(),
            bootstrap->GetLogging(),
            bootstrap->GetStorage(),
            CreateServerStatsStub(),
            CreateErrorHandlerStub(),
            options);

        auto handler = factory->CreateHandler();

        TStringStream in;
        TStringStream out;

        NegotiateClient(*handler, in, out);

        bootstrap->Stop();
    }

    Y_UNIT_TEST(ShouldHandleRequests)
    {
        auto storage = std::make_shared<TTestStorage>();
        SetupStorage(*storage);

        auto bootstrap = CreateBootstrap(storage);
        bootstrap->Start();

        TStorageOptions options;
        options.DiskId = DefaultDiskId;
        options.BlockSize = DefaultBlockSize;
        options.BlocksCount = DefaultBlocksCount;

        auto factory = CreateServerHandlerFactory(
            CreateDefaultDeviceHandlerFactory(),
            bootstrap->GetLogging(),
            bootstrap->GetStorage(),
            CreateServerStatsStub(),
            CreateErrorHandlerStub(),
            options);

        auto handler = factory->CreateHandler();

        TStringStream in;
        TStringStream out;

        NegotiateClient(*handler, in, out);
        ProcessRequests(*handler, in, out);
        ProcessRequests(*handler, in, out, 0);

        bootstrap->Stop();
    }

    Y_UNIT_TEST(ShouldSendMinBlockSizeToClientIfNeeded)
    {
        auto storage = std::make_shared<TTestStorage>();
        SetupStorage(*storage);

        auto bootstrap = CreateBootstrap(storage);
        bootstrap->Start();

        const ui32 defaultMinBlockSize = 512;
        const ui32 blockSize = 8192;

        TStorageOptions options;
        options.DiskId = DefaultDiskId;
        options.BlockSize = blockSize;
        options.BlocksCount = DefaultBlocksCount;

        {
            auto factory = CreateServerHandlerFactory(
                CreateDefaultDeviceHandlerFactory(),
                bootstrap->GetLogging(),
                bootstrap->GetStorage(),
                CreateServerStatsStub(),
                CreateErrorHandlerStub(),
                options);

            auto handler = factory->CreateHandler();

            TStringStream in;
            TStringStream out;

            auto exportInfo = NegotiateClient(*handler, in, out);
            UNIT_ASSERT_VALUES_EQUAL(defaultMinBlockSize, exportInfo.MinBlockSize);
            UNIT_ASSERT_VALUES_EQUAL(blockSize, exportInfo.OptBlockSize);
        }

        {
            options.UnalignedRequestsDisabled = true;

            auto factory = CreateServerHandlerFactory(
                CreateDefaultDeviceHandlerFactory(),
                bootstrap->GetLogging(),
                bootstrap->GetStorage(),
                CreateServerStatsStub(),
                CreateErrorHandlerStub(),
                options);

            auto handler = factory->CreateHandler();

            TStringStream in;
            TStringStream out;

            auto exportInfo = NegotiateClient(*handler, in, out);
            UNIT_ASSERT_VALUES_EQUAL(blockSize, exportInfo.MinBlockSize);
            UNIT_ASSERT_VALUES_EQUAL(blockSize, exportInfo.OptBlockSize);
        }

        {
            options.UnalignedRequestsDisabled = false;
            options.SendMinBlockSize = true;

            auto factory = CreateServerHandlerFactory(
                CreateDefaultDeviceHandlerFactory(),
                bootstrap->GetLogging(),
                bootstrap->GetStorage(),
                CreateServerStatsStub(),
                CreateErrorHandlerStub(),
                options);

            auto handler = factory->CreateHandler();

            TStringStream in;
            TStringStream out;

            auto exportInfo = NegotiateClient(*handler, in, out);
            UNIT_ASSERT_VALUES_EQUAL(blockSize, exportInfo.MinBlockSize);
            UNIT_ASSERT_VALUES_EQUAL(blockSize, exportInfo.OptBlockSize);
        }

        bootstrap->Stop();
    }

    Y_UNIT_TEST(ShouldPassCorrectMetrics)
    {
        auto storage = std::make_shared<TTestStorage>();
        SetupStorage(*storage);

        auto bootstrap = CreateBootstrap(storage);
        bootstrap->Start();

        TStorageOptions options;
        options.DiskId = DefaultDiskId;
        options.BlockSize = DefaultBlockSize;
        options.BlocksCount = DefaultBlocksCount;

        auto serverStats = std::make_shared<TTestServerStats>();

        bool expectedUnaligned = false;
        ui64 expectedStartIndex = 0;
        ui64 expectedBlockCount = 0;

        ui32 requestCounter = 0;
        ui32 expectedRequestCounter = 0;

        serverStats->PrepareMetricRequestHandler = [&] (
            TMetricRequest& metricRequest,
            TString clientId,
            TString diskId,
            ui64 startIndex,
            ui32 requestBytes,
            bool unaligned)
        {
            Y_UNUSED(clientId);

            UNIT_ASSERT(diskId == DefaultDiskId);
            metricRequest.DiskId = std::move(diskId);

            UNIT_ASSERT_VALUES_EQUAL(expectedUnaligned, unaligned);

            switch (metricRequest.RequestType)
            {
                case EBlockStoreRequest::ReadBlocks:
                case EBlockStoreRequest::WriteBlocks:
                case EBlockStoreRequest::ZeroBlocks:
                    UNIT_ASSERT_VALUES_EQUAL(expectedStartIndex, startIndex);
                    UNIT_ASSERT_VALUES_EQUAL(expectedBlockCount * DefaultBlockSize, requestBytes);
                    break;
                case EBlockStoreRequest::MountVolume:
                case EBlockStoreRequest::UnmountVolume:
                    break;
                default:
                    UNIT_FAIL("Unexpected request");
                    break;
            }

            ++requestCounter;
        };

        auto factory = CreateServerHandlerFactory(
            CreateDefaultDeviceHandlerFactory(),
            bootstrap->GetLogging(),
            bootstrap->GetStorage(),
            serverStats,
            CreateErrorHandlerStub(),
            options);

        auto handler = factory->CreateHandler();

        TStringStream in;
        TStringStream out;

        NegotiateClient(*handler, in, out);

        expectedUnaligned = false;
        expectedStartIndex = 0;
        expectedBlockCount = 1;
        expectedRequestCounter += 3;

        ProcessRequests(*handler, in, out);
        UNIT_ASSERT_VALUES_EQUAL(expectedRequestCounter, requestCounter);

        expectedUnaligned = true;
        expectedStartIndex = 1;
        expectedBlockCount = 2;
        expectedRequestCounter += 3;
        ProcessUnalignedRequests(*handler, in, out);

        bootstrap->Stop();
    }

    // TODO: simple/structured
}

}   // namespace NCloud::NBlockStore::NBD
