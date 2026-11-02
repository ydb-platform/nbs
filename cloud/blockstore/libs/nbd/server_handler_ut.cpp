#include "server_handler.h"

#include "error_handler.h"
#include "protocol.h"
#include "utils.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/diagnostics/server_stats_test.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/device_handler.h>
#include <cloud/blockstore/libs/service/storage_test.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/yexception.h>
#include <util/stream/str.h>

#include <array>

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

    void ExecutePending()
    {
        auto tasks = std::move(PendingTasks);
        for (auto& task: tasks) {
            task->Execute();
        }
    }

    TServerContext(IOutputStream& out)
        : Out(out)
    {}

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
        return future.GetValue(TDuration::Max());
    }

    const NProto::TWriteBlocksLocalResponse& WaitFor(
        const TFuture<NProto::TWriteBlocksLocalResponse>& future) override
    {
        return future.GetValue(TDuration::Max());
    }

    const NProto::TZeroBlocksResponse& WaitFor(
        const TFuture<NProto::TZeroBlocksResponse>& future) override
    {
        return future.GetValue(TDuration::Max());
    }

    void SendResponse(TServerResponsePtr response) override
    {
        if (Handler) {
            LastResponse = response;
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

class TRequestMetricsFixture
{
public:
    ui32 Current = 0;
    ui32 Completed = 0;
    ui32 Errors = 0;
    std::shared_ptr<TTestStorage> Storage = std::make_shared<TTestStorage>();
    IServerHandlerPtr Handler;
    TStringStream Replies;
    TStringStream Requests;
    TIntrusivePtr<TServerContext> Context;

    TRequestMetricsFixture()
    {
        Storage->DoAllocations = true;
        SetupStorage(*Storage);
        auto stats = std::make_shared<TTestServerStats>();
        stats->RequestStartedHandler = [this](auto&, auto&, auto&, const auto&)
        {
            ++Current;
        };
        stats->RequestCompletedHandler =
            [this](auto&, auto&, auto&, const auto& error)
        {
            UNIT_ASSERT(Current);
            --Current;
            ++Completed;
            Errors += HasError(error);
        };
        TStorageOptions options;
        options.DiskId = DefaultDiskId;
        options.BlockSize = DefaultBlockSize;
        options.BlocksCount = DefaultBlocksCount;
        Handler = CreateServerHandlerFactory(
                      CreateDefaultDeviceHandlerFactory(),
                      CreateLoggingService("console", {TLOG_DEBUG}),
                      Storage,
                      std::move(stats),
                      CreateErrorHandlerStub(),
                      options)
                      ->CreateHandler();
        NegotiateClient(*Handler, Replies, Requests);
        Context = MakeIntrusive<TServerContext>(Replies);
        Context->Handler = Handler.get();
        Context->Deferred = true;
    }

    void Accept(ui32 command, bool truncate = false)
    {
        TRequest request{};
        request.Magic = NBD_REQUEST_MAGIC;
        request.Type = command;
        request.Handle = 1;
        request.Length = DefaultBlockSize;
        TStringStream encoded;
        TRequestWriter writer(encoded);
        writer.WriteRequest(
            request,
            command == NBD_CMD_WRITE ? TString(request.Length, 'a')
                                     : TString());
        const auto& bytes = encoded.Str();
        Requests.Write(bytes.data(), bytes.size() - truncate);
        Handler->ProcessRequests(Context, Requests, Replies, nullptr);
    }
};

class TThrowingResponseOutput final: public IOutputStream
{
    void DoWrite(const void*, size_t) override
    {
        ythrow yexception() << "response write failed";
    }
};

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
    Y_UNIT_TEST(ShouldBalancePendingRequestMetrics)
    {
        for (const ui32 command:
             {NBD_CMD_READ, NBD_CMD_WRITE, NBD_CMD_WRITE_ZEROES})
        {
            for (const bool disconnect: {false, true}) {
                TRequestMetricsFixture fixture;
                fixture.Accept(command);
                UNIT_ASSERT_VALUES_EQUAL(fixture.Current, 1);
                UNIT_ASSERT_VALUES_EQUAL(fixture.Completed, 0);

                if (disconnect) {
                    fixture.Handler->CancelRequests();
                    UNIT_ASSERT_VALUES_EQUAL(fixture.Current, 0);
                    UNIT_ASSERT_VALUES_EQUAL(fixture.Completed, 1);
                }
                fixture.Context->ExecutePending();
                UNIT_ASSERT_VALUES_EQUAL(fixture.Current, 0);
                UNIT_ASSERT_VALUES_EQUAL(fixture.Completed, 1);
                UNIT_ASSERT_VALUES_EQUAL(fixture.Errors, disconnect);
            }
        }
    }

    Y_UNIT_TEST(ShouldKeepMetricsUntilSendStopsAfterReceiveError)
    {
        TRequestMetricsFixture fixture;
        fixture.Accept(NBD_CMD_READ);
        fixture.Handler->ProcessException(
            std::make_exception_ptr(yexception() << "invalid next request"));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Current, 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Completed, 0);

        fixture.Context->ExecutePending();
        fixture.Handler->CancelRequests();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Current, 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Completed, 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Errors, 0);
    }

    Y_UNIT_TEST(ShouldCompleteMetricsForStorageError)
    {
        TRequestMetricsFixture fixture;
        fixture.Storage->ReadBlocksLocalHandler = [](auto, auto)
        {
            NProto::TReadBlocksLocalResponse response;
            *response.MutableError() = MakeError(E_IO, "read failure");
            return MakeFuture(response);
        };
        fixture.Accept(NBD_CMD_READ);
        fixture.Context->ExecutePending();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Current, 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Completed, 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Errors, 1);
    }

    Y_UNIT_TEST(ShouldCompleteMetricsWhenResponseIsNotSent)
    {
        for (const bool failedWrite: {false, true}) {
            TRequestMetricsFixture fixture;
            fixture.Context->ForwardResponses = false;
            fixture.Accept(NBD_CMD_READ);
            fixture.Context->ExecutePending();
            UNIT_ASSERT_VALUES_EQUAL(fixture.Current, 1);
            auto& response = *fixture.Context->LastResponse;
            if (failedWrite) {
                TThrowingResponseOutput output;
                UNIT_ASSERT_EXCEPTION(
                    fixture.Handler->SendResponse(output, response),
                    yexception);
            } else {
                fixture.Handler->CompleteResponse(response);
            }
            fixture.Handler->CompleteResponse(response);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Current, 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Completed, 1);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Errors, failedWrite);
        }
    }

    Y_UNIT_TEST(ShouldCompleteMetricsForTruncatedWritePayload)
    {
        TRequestMetricsFixture fixture;
        UNIT_ASSERT_EXCEPTION(fixture.Accept(NBD_CMD_WRITE, true), yexception);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Current, 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Completed, 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Errors, 1);
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
