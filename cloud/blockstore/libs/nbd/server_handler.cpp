#include "server_handler.h"

#include "error_handler.h"
#include "protocol.h"
#include "utils.h"

#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/device_handler.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/coroutine/engine/impl.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NBD {

using namespace NMonitoring;

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

inline TRequestInfo CreateRequestInfo(const TRequestContext& ctx)
{
    return TRequestInfo(
        ctx.MetricRequest.RequestType,
        ctx.CallContext->RequestId,
        ctx.MetricRequest.DiskId,
        ctx.MetricRequest.ClientId);
}

////////////////////////////////////////////////////////////////////////////////

class TServerHandler final
    : public IServerHandler
{
private:
    const ILoggingServicePtr Logging;
    const IServerStatsPtr ServerStats;
    const IDeviceHandlerPtr DeviceHandler;
    const IErrorHandlerPtr ErrorHandler;
    const TStorageOptions Options;

    TLog Log;

    TIntrusiveList<TRequestContext> RequestsInFlight;

    bool StructuredReply = false;
    bool UseNbsErrors = false;

public:
    TServerHandler(
            ILoggingServicePtr logging,
            IServerStatsPtr serverStats,
            IDeviceHandlerPtr deviceHandler,
            IErrorHandlerPtr errorHandler,
            const TStorageOptions& options)
        : Logging(std::move(logging))
        , ServerStats(std::move(serverStats))
        , DeviceHandler(std::move(deviceHandler))
        , ErrorHandler(std::move(errorHandler))
        , Options(options)
    {
        Log = Logging->CreateLog("BLOCKSTORE_NBD");
    }

    bool NegotiateClient(IInputStream& in, IOutputStream& out) override
    {
        TRequestReader reader(in);
        TRequestWriter writer(out);

        return NegotiateClient(reader, writer);
    }

    void SendResponse(
        IOutputStream& out,
        TServerResponse& response) override
    {
        out.Write(
            response.HeaderBuffer.Data(),
            response.HeaderBuffer.Size());

        if (response.DataBuffer) {
            out.Write(
                response.DataBuffer.get(),
                response.RequestBytes);
        }

        UnregisterRequest(response.RequestContext, response.Error);
    }

    void ProcessRequests(
        IServerContextPtr ctx,
        IInputStream& in,
        IOutputStream& out,
        TCont* cont) override
    {
        TRequestReader reader(in);
        TRequestWriter writer(out);

        return ProcessRequests(std::move(ctx), reader, writer, cont);
    }

    size_t CollectRequests(
        const TIncompleteRequestsCollector& collector) override;

    void ProcessException(std::exception_ptr e) override
    {
        ErrorHandler->ProcessException(e);
    }

private:
    bool NegotiateClient(TRequestReader& in, TRequestWriter& out);

    bool ProcessOptions(TRequestReader& in, TRequestWriter& out);

    void ProcessExportListRequest(
        const TOption& option,
        TRequestWriter& out);

    void ProcessExportInfoRequest(
        const TOption& option,
        TRequestReader& in,
        TRequestWriter& out);

    void ProcessRequests(
        IServerContextPtr ctx,
        TRequestReader& in,
        TRequestWriter& out,
        TCont* cont);

    void ProcessReadRequest(
        IServerContextPtr ctx,
        TRequestContextPtr requestCtx,
        const TRequest& request);

    void ProcessWriteRequest(
        IServerContextPtr ctx,
        TRequestContextPtr requestCtx,
        const TRequest& request,
        TStorageBuffer requestData);

    void ProcessZeroRequest(
        IServerContextPtr ctx,
        TRequestContextPtr requestCtx,
        const TRequest& request);

    TRequestContextPtr RegisterRequest(
        const TRequest& request,
        EBlockStoreRequest requestType);

    void UnregisterRequest(
        const TRequestContextPtr& requestCtx,
        const NProto::TError& error);

    ui32 GetNbdErrorCode(ui32 nbsCode);

private:
    void WriteOptionReply(
        TRequestWriter& out,
        ui32 option,
        ui32 type,
        TStringBuf replyData = {})
    {
        STORAGE_DEBUG(Options
            << " SEND OptionReply"
            << " option:" << option
            << " type:" << type
            << " length:" << replyData.size());
        out.WriteOptionReply(option, type, replyData);
    }

    void WriteOptionError(
        TRequestWriter& out,
        ui32 option,
        ui32 type,
        TStringBuf message)
    {
        STORAGE_DEBUG(Options
            << " SEND OptionReply"
            << " option:" << option
            << " type:" << type
            << " message:" << TString(message).Quote());
        out.WriteOptionReply(option, type, message);
    }

    void WriteSimpleReply(
        TRequestWriter& out,
        ui64 handle,
        ui32 error,
        TStringBuf message)
    {
        if (error) {
            STORAGE_ERROR(Options
                << " SEND SimpleReply"
                << " #" << handle
                << " error:" << error
                << " message:" << TString(message).Quote());
        } else {
            STORAGE_DEBUG(Options << " SEND SimpleReply #" << handle);
        }
        out.WriteSimpleReply(handle, error);
    }

    void WriteStructuredError(
        TRequestWriter& out,
        ui64 handle,
        ui32 error,
        TStringBuf message)
    {
        STORAGE_ERROR(Options
            << " SEND StructuredError"
            << " #" << handle
            << " error:" << error
            << " message:" << TString(message).Quote());
        out.WriteStructuredError(handle, error, message);
    }

    void WriteStructuredDone(TRequestWriter& out, ui64 handle)
    {
        STORAGE_DEBUG(Options
            << " SEND StructuredDone"
            << " #" << handle);
        out.WriteStructuredDone(handle);
    }

    void WriteStructuredReadData(
        TRequestWriter& out,
        ui64 handle,
        ui64 offset,
        ui32 length,
        bool final)
    {
        STORAGE_DEBUG(Options
            << " SEND StructuredReadData"
            << " #" << handle
            << " offset:" << offset
            << " length:" << length
            << " final:" << final);
        out.WriteStructuredReadData(handle, offset, length, final);
    }

    void WriteGenericReply(TRequestWriter& out, ui64 handle)
    {
        if (StructuredReply) {
            WriteStructuredDone(out, handle);
        } else {
            WriteSimpleReply(out, handle, 0, {});
        }
    }

    void WriteGenericError(
        TRequestWriter& out,
        ui64 handle,
        ui32 error,
        TStringBuf message)
    {
        if (StructuredReply) {
            WriteStructuredError(out, handle, error, message);
        } else {
            WriteSimpleReply(out, handle, error, message);
        }
    }

    bool ReadOption(
        TRequestReader& in,
        TOption& option,
        TBuffer& optionData)
    {
        if (in.ReadOption(option, optionData)) {
            STORAGE_DEBUG(Options
                << " RECEIVE Option"
                << " option:" << option.Option
                << " length:" << option.Length);
            return true;
        }

        return false;
    }

    bool ReadRequest(TRequestReader& in, TRequest& request)
    {
        if (in.ReadRequest(request)) {
            STORAGE_DEBUG(Options
                << " RECEIVE Request"
                << " #" << request.Handle
                << " type:" << request.Type
                << " length:" << request.Length);
            return true;
        }

        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

bool TServerHandler::NegotiateClient(TRequestReader& in, TRequestWriter& out)
{
    STORAGE_DEBUG(Options << " Negotiate client connection");

    out.WriteServerHello(NBD_FLAG_FIXED_NEWSTYLE | NBD_FLAG_NO_ZEROES);

    TClientHello client;
    if (in.ReadClientHello(client)) {
        Y_ENSURE(client.Flags == (NBD_FLAG_C_FIXED_NEWSTYLE | NBD_FLAG_C_NO_ZEROES));

        if (ProcessOptions(in, out)) {
            return true;
        }
    }

    return false;
}

bool TServerHandler::ProcessOptions(TRequestReader& in, TRequestWriter& out)
{
    TOption option;
    TBuffer optionData;

    auto replyExceptionError = [&] (const std::exception& e) {
        WriteOptionError(
            out,
            option.Option,
            NBD_REP_ERR_INVALID,
            TStringBuilder()
                << "invalid option request: " << e.what());
    };

    auto replyUnsupportedError = [&] {
        WriteOptionError(
            out,
            option.Option,
            NBD_REP_ERR_UNSUP,
            TStringBuilder()
                << "unsupported option request: " << option.Option
                << " (dataSize = " << optionData.Size() << ")");
    };

    while (ReadOption(in, option, optionData)) {
        switch (option.Option) {
            case NBD_OPT_ABORT:
                WriteOptionReply(out, option.Option, NBD_REP_ACK);
                return false;

            case NBD_OPT_LIST:
                try {
                    Y_ENSURE(!optionData);
                    ProcessExportListRequest(option, out);
                } catch (const std::exception& e) {
                    replyExceptionError(e);
                }
                break;

            case NBD_OPT_INFO:
            case NBD_OPT_GO:
                try {
                    TBufferRequestReader optionIn(optionData);
                    ProcessExportInfoRequest(option, optionIn, out);

                    if (option.Option == NBD_OPT_GO) {
                        return true;
                    }
                } catch (const std::exception& e) {
                    replyExceptionError(e);
                }
                break;

            case NBD_OPT_STRUCTURED_REPLY:
                try {
                    Y_ENSURE(!optionData);
                    Y_ENSURE(!StructuredReply);
                    StructuredReply = true;
                    WriteOptionReply(out, option.Option, NBD_REP_ACK);
                } catch (const std::exception& e) {
                    replyExceptionError(e);
                }
                break;

            case NBD_OPT_USE_NBS_ERRORS:
                try {
                    if (AsStringBuf(optionData) == NBS_OPTION_TAG) {
                        Y_ENSURE(!UseNbsErrors);
                        UseNbsErrors = true;
                        WriteOptionReply(out, option.Option, NBD_REP_ACK);
                    } else {
                        replyUnsupportedError();
                    }
                } catch (const std::exception& e) {
                    replyExceptionError(e);
                }
                break;

            default:
                replyUnsupportedError();
                break;
        }
    }

    return false;
}

void TServerHandler::ProcessExportListRequest(
    const TOption& option,
    TRequestWriter& out)
{
    TExportInfo exp {};
    exp.Name = Options.DiskId;

    TBufferRequestWriter response;
    response.WriteExportList(exp);

    WriteOptionReply(
        out,
        option.Option,
        NBD_REP_SERVER,
        AsStringBuf(response.Buffer()));

    WriteOptionReply(out, option.Option, NBD_REP_ACK);
}

void TServerHandler::ProcessExportInfoRequest(
    const TOption& option,
    TRequestReader& in,
    TRequestWriter& out)
{
    TExportInfoRequest request;
    if (!in.ReadExportInfoRequest(request)) {
        ythrow yexception() << "Unexpected end of stream";
    }

    if (request.Name && request.Name != Options.DiskId) {
        WriteOptionError(
            out,
            option.Option,
            NBD_REP_ERR_UNKNOWN,
            TStringBuilder()
                << "unknown export: " << request.Name);
        return;
    }

    bool sendName = false;
    bool sendBlockSize = false;

    for (ui16 type: request.InfoTypes) {
        switch (type) {
            case NBD_INFO_NAME:
                sendName = true;
                break;

            case NBD_INFO_BLOCK_SIZE:
                sendBlockSize = true;
                break;
        }
    }

    TExportInfo exp {};
    exp.Name = Options.DiskId;
    exp.Size = Options.BlocksCount * Options.BlockSize;
    exp.MinBlockSize = Options.UnalignedRequestsDisabled ? Options.BlockSize : 512;
    exp.OptBlockSize = DefaultBlockSize;
    exp.MaxBlockSize = NBD_MAX_BUFFER_SIZE;

    exp.Flags = NBD_FLAG_HAS_FLAGS
              | NBD_FLAG_SEND_TRIM
              | NBD_FLAG_SEND_WRITE_ZEROES;

    if (Options.CheckpointId) {
        exp.Flags |= NBD_FLAG_READ_ONLY;
    }

    if (sendBlockSize) {
        if (Options.SendMinBlockSize) {
            exp.MinBlockSize = Options.BlockSize;
        }
        exp.OptBlockSize = Options.BlockSize;
    } else {
        if (option.Option == NBD_OPT_INFO) {
            // client MUST request NBD_INFO_BLOCK_SIZE
            WriteOptionError(
                out,
                option.Option,
                NBD_REP_ERR_BLOCK_SIZE_REQD,
                "request NBD_INFO_BLOCK_SIZE to use this export");
            return;
        }
    }

    TVector<ui16> infoTypes;
    if (sendName) {
        infoTypes.push_back(NBD_INFO_NAME);
    }
    infoTypes.push_back(NBD_INFO_BLOCK_SIZE);
    infoTypes.push_back(NBD_INFO_EXPORT);

    for (ui16 type: infoTypes) {
        TBufferRequestWriter response;
        response.WriteExportInfo(exp, type);

        WriteOptionReply(
            out,
            option.Option,
            NBD_REP_INFO,
            AsStringBuf(response.Buffer()));
    }

    WriteOptionReply(out, option.Option, NBD_REP_ACK);
}

////////////////////////////////////////////////////////////////////////////////

void TServerHandler::ProcessRequests(
    IServerContextPtr ctx,
    TRequestReader& in,
    TRequestWriter& out,
    TCont* cont)
{
    TRequest request;

    auto replyError = [&] (int error, const TString& message) {
        WriteGenericError(out, request.Handle, GetNbdErrorCode(error), message);
    };

    const auto cancelError = MakeError(E_CANCELLED, "NbdServer was stopped");

    while (ReadRequest(in, request)) {
        switch (request.Type) {
            case NBD_CMD_READ: {
                auto requestCtx = RegisterRequest(
                    request,
                    EBlockStoreRequest::ReadBlocks);

                if (!ctx->AcquireRequest(request.Length)) {
                    // no need to reply, cause server is shutting down
                    UnregisterRequest(requestCtx, cancelError);
                    break;
                }

                ServerStats->RequestAcquired(
                    requestCtx->MetricRequest,
                    *requestCtx->CallContext);

                ctx->ExecuteSimple([=, this] () mutable {
                    ProcessReadRequest(
                        std::move(ctx),
                        std::move(requestCtx),
                        request);
                });
                break;
            }

            case NBD_CMD_WRITE: {
                if (Options.CheckpointId) {
                    replyError(E_ARGUMENT, "invalid write request");
                    break;
                }

                auto requestCtx = RegisterRequest(
                    request,
                    EBlockStoreRequest::WriteBlocks);

                if (!ctx->AcquireRequest(request.Length)) {
                    // no need to reply, cause server is shutting down
                    UnregisterRequest(requestCtx, cancelError);
                    break;
                }

                ServerStats->RequestAcquired(
                    requestCtx->MetricRequest,
                    *requestCtx->CallContext);

                auto requestData = DeviceHandler->AllocateBuffer(request.Length);
                if (request.Length) {
                    in.ReadOrFail(requestData.get(), request.Length);
                }

                ctx->ExecuteSimple(
                    [=, this, data = std::move(requestData)] () mutable {
                        ProcessWriteRequest(
                            std::move(ctx),
                            std::move(requestCtx),
                            request,
                            std::move(data));
                    });
                break;
            }

            case NBD_CMD_TRIM:
            case NBD_CMD_WRITE_ZEROES: {
                if (Options.CheckpointId) {
                    replyError(E_ARGUMENT, "invalid zero request");
                    break;
                }

                auto requestCtx = RegisterRequest(
                    request,
                    EBlockStoreRequest::ZeroBlocks);

                if (!ctx->AcquireRequest(request.Length)) {
                    // no need to reply, cause server is shutting down
                    UnregisterRequest(requestCtx, cancelError);
                    break;
                }

                ServerStats->RequestAcquired(
                    requestCtx->MetricRequest,
                    *requestCtx->CallContext);

                ctx->ExecuteSimple([=, this] () mutable {
                    ProcessZeroRequest(
                        std::move(ctx),
                        std::move(requestCtx),
                        request);
                });
                break;
            }

            case NBD_CMD_DISC:
                // soft-disconnect
                return;

            default:
                replyError(E_ARGUMENT, TStringBuilder()
                    << "unsupported request type: " << request.Type);
                break;
        }

        if (cont) {
            cont->Yield();
        }
    }
}

void TServerHandler::ProcessReadRequest(
    IServerContextPtr ctx,
    TRequestContextPtr requestCtx,
    const TRequest& request)
{
    STORAGE_DEBUG(CreateRequestInfo(*requestCtx)
        << " PROCESS ReadRequest"
        << " #" << request.Handle
        << " offset:" << request.From
        << " length:" << request.Length);

    auto responseData = DeviceHandler->AllocateBuffer(request.Length);

    NProto::TError error;
    if (request.Length) {
        auto guardedSgList = TGuardedSgList({
            { responseData.get(), request.Length }
        });

        auto future = DeviceHandler->Read(
            requestCtx->CallContext,
            request.From,
            request.Length,
            guardedSgList,
            Options.CheckpointId);

        const auto& response = ctx->WaitFor(future);
        error = response.GetError();

        guardedSgList.Close();
    }

    ServerStats->ResponseSent(requestCtx->MetricRequest, *requestCtx->CallContext);
    STORAGE_DEBUG(CreateRequestInfo(*requestCtx)
        << " PROCESS ReadResponse"
        << " #" << request.Handle
        << " offset:" << request.From
        << " length:" << request.Length);

    TBufferRequestWriter out;
    if (!HasError(error)) {
        if (StructuredReply) {
            WriteStructuredReadData(
                out,
                request.Handle,
                request.From,
                request.Length,
                true);
        } else {
            WriteGenericReply(out, request.Handle);
        }
    } else {
        WriteGenericError(
            out,
            request.Handle,
            GetNbdErrorCode(error.GetCode()),
            TStringBuilder()
                << "read request failed: " << FormatError(error));
        responseData = {};
    }

    auto serverResponse = MakeIntrusive<TServerResponse>(
        std::move(requestCtx),
        error,
        request.Length,
        std::move(out.Buffer()),
        std::move(responseData));

    ctx->SendResponse(std::move(serverResponse));
}

void TServerHandler::ProcessWriteRequest(
    IServerContextPtr ctx,
    TRequestContextPtr requestCtx,
    const TRequest& request,
    TStorageBuffer requestData)
{
    STORAGE_DEBUG(CreateRequestInfo(*requestCtx)
        << " PROCESS WriteRequest"
        << " #" << request.Handle
        << " offset:" << request.From
        << " length:" << request.Length);

    NProto::TError error;
    if (request.Length) {
        auto guardedSgList = TGuardedSgList({
            { requestData.get(), request.Length }
        });

        auto future = DeviceHandler->Write(
            requestCtx->CallContext,
            request.From,
            request.Length,
            guardedSgList);

        const auto& response = ctx->WaitFor(future);
        error = response.GetError();

        guardedSgList.Close();
        requestData.reset();
    }

    ServerStats->ResponseSent(requestCtx->MetricRequest, *requestCtx->CallContext);
    STORAGE_DEBUG(CreateRequestInfo(*requestCtx)
        << " PROCESS WriteResponse"
        << " #" << request.Handle
        << " offset:" << request.From
        << " length:" << request.Length);

    TBufferRequestWriter out;
    if (!HasError(error)) {
        WriteGenericReply(out, request.Handle);
    } else {
        WriteGenericError(
            out,
            request.Handle,
            GetNbdErrorCode(error.GetCode()),
            TStringBuilder()
                << "write request failed: " << FormatError(error));
    }

    auto serverResponse = MakeIntrusive<TServerResponse>(
        std::move(requestCtx),
        error,
        request.Length,
        std::move(out.Buffer()));

    ctx->SendResponse(std::move(serverResponse));
}

void TServerHandler::ProcessZeroRequest(
    IServerContextPtr ctx,
    TRequestContextPtr requestCtx,
    const TRequest& request)
{
    STORAGE_DEBUG(CreateRequestInfo(*requestCtx)
        << " PROCESS ZeroRequest"
        << " #" << request.Handle
        << " offset:" << request.From
        << " length:" << request.Length);

    NProto::TError error;
    if (request.Length) {
        auto future = DeviceHandler->Zero(
            requestCtx->CallContext,
            request.From,
            request.Length);

        const auto& response = ctx->WaitFor(future);
        error = response.GetError();
    }

    ServerStats->ResponseSent(requestCtx->MetricRequest, *requestCtx->CallContext);
    STORAGE_DEBUG(CreateRequestInfo(*requestCtx)
        << " PROCESS ZeroResponse"
        << " #" << request.Handle
        << " offset:" << request.From
        << " length:" << request.Length);

    TBufferRequestWriter out;
    if (!HasError(error)) {
        WriteGenericReply(out, request.Handle);
    } else {
        WriteGenericError(
            out,
            request.Handle,
            GetNbdErrorCode(error.GetCode()),
            TStringBuilder()
                << "zero request failed: " << FormatError(error));
    }

    auto serverResponse = MakeIntrusive<TServerResponse>(
        std::move(requestCtx),
        error,
        request.Length,
        std::move(out.Buffer()));

    ctx->SendResponse(std::move(serverResponse));
}

TRequestContextPtr TServerHandler::RegisterRequest(
    const TRequest& request,
    EBlockStoreRequest requestType)
{
    auto startIndex = request.From / Options.BlockSize;
    auto endIndex = (request.From + request.Length) / Options.BlockSize;
    if (endIndex * Options.BlockSize < request.From + request.Length) {
        ++endIndex;
    }
    bool unaligned =
        startIndex * Options.BlockSize != request.From ||
        endIndex * Options.BlockSize != request.From + request.Length;

    auto requestCtx = MakeIntrusive<TRequestContext>(request.Handle, requestType);

    ServerStats->PrepareMetricRequest(
        requestCtx->MetricRequest,
        Options.ClientId,
        Options.DiskId,
        startIndex,
        ServerStats->GetBlockSize(Options.DiskId) * (endIndex - startIndex),
        unaligned);

    ServerStats->RequestStarted(
        Log,
        requestCtx->MetricRequest,
        *requestCtx->CallContext);

    RequestsInFlight.PushBack(requestCtx.Get());

    return requestCtx;
}

void TServerHandler::UnregisterRequest(
    const TRequestContextPtr& requestCtx,
    const NProto::TError& error)
{
    requestCtx->Unlink();

    ServerStats->RequestCompleted(
        Log,
        requestCtx->MetricRequest,
        *requestCtx->CallContext,
        error);
}

size_t TServerHandler::CollectRequests(
    const TIncompleteRequestsCollector& collector)
{
    ui64 now = GetCycleCount();
    size_t count = 0;
    for (auto& request : RequestsInFlight) {
        ++count;
        auto requestTime = request.CallContext->CalcRequestTime(now);
        if (requestTime) {
            collector(
                *request.CallContext,
                request.MetricRequest.VolumeInfo,
                request.MetricRequest.MediaKind,
                request.MetricRequest.RequestType,
                requestTime);
        }
    }
    return count;
}

ui32 TServerHandler::GetNbdErrorCode(ui32 nbsCode)
{
    if (UseNbsErrors) {
        return nbsCode;
    }

    switch (nbsCode) {
        case E_ARGUMENT:
            return NBD_EINVAL;
        case E_UNAUTHORIZED:
            return NBD_EPERM;
        default:
            return NBD_EIO;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TServerHandlerFactory final
    : public IServerHandlerFactory
{
private:
    const ILoggingServicePtr Logging;
    const IServerStatsPtr ServerStats;
    const IDeviceHandlerPtr DeviceHandler;
    const IErrorHandlerPtr ErrorHandler;
    const TStorageOptions Options;

public:
    TServerHandlerFactory(
            ILoggingServicePtr logging,
            IServerStatsPtr serverStats,
            IDeviceHandlerPtr deviceHandler,
            IErrorHandlerPtr errorHandler,
            const TStorageOptions& options)
        : Logging(std::move(logging))
        , ServerStats(std::move(serverStats))
        , DeviceHandler(std::move(deviceHandler))
        , ErrorHandler(std::move(errorHandler))
        , Options(options)
    {}

    IServerHandlerPtr CreateHandler() override
    {
        return std::make_shared<TServerHandler>(
            Logging,
            ServerStats,
            DeviceHandler,
            ErrorHandler,
            Options);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IServerHandlerFactoryPtr CreateServerHandlerFactory(
    IDeviceHandlerFactoryPtr deviceHandlerFactory,
    ILoggingServicePtr logging,
    IStoragePtr storage,
    IServerStatsPtr serverStats,
    IErrorHandlerPtr errorHandler,
    const TStorageOptions& options)
{
    TDeviceHandlerParams params{
        .Storage = std::move(storage),
        .DiskId = options.DiskId,
        .ClientId = options.ClientId,
        .BlockSize = options.BlockSize,
        .MaxZeroBlocksSubRequestSize = options.MaxZeroBlocksSubRequestSize,
        .CheckBufferModificationDuringWriting =
            options.CheckBufferModificationDuringWriting,
        .UnalignedRequestsDisabled = options.UnalignedRequestsDisabled,
        .StorageMediaKind = options.StorageMediaKind};

    auto deviceHandler =
        deviceHandlerFactory->CreateDeviceHandler(std::move(params));

    return std::make_shared<TServerHandlerFactory>(
        std::move(logging),
        std::move(serverStats),
        std::move(deviceHandler),
        std::move(errorHandler),
        options);
}

////////////////////////////////////////////////////////////////////////////////

IOutputStream& operator <<(IOutputStream& out, const TStorageOptions& options)
{
    if (options.DiskId) {
        out << "[d:" << options.DiskId << "] ";
    }

    if (options.ClientId) {
        out << "[c:" << options.ClientId << "] ";
    }

    return out;
}

}   // namespace NCloud::NBlockStore::NBD
