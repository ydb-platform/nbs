#include "client_handler.h"

#include "protocol.h"
#include "utils.h"

#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/datetime/cputimer.h>
#include <util/generic/hash.h>
#include <util/generic/set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NBD {

namespace {

////////////////////////////////////////////////////////////////////////////////

ui32 OptionReplyTypeToProtoErrorCode(ui32 type)
{
    Y_ENSURE(OptionReplyTypeIsError(type));

    switch (type) {
        case NBD_REP_ERR_UNSUP:
            return E_NOT_IMPLEMENTED;
        case NBD_REP_ERR_POLICY:
            return E_UNAUTHORIZED;
        case NBD_REP_ERR_INVALID:
            return E_ARGUMENT;
        case NBD_REP_ERR_PLATFORM:
            return E_INVALID_STATE;
        case NBD_REP_ERR_TLS_REQD:
            return E_INVALID_STATE;
        case NBD_REP_ERR_UNKNOWN:
            return E_FAIL;
        case NBD_REP_ERR_SHUTDOWN:
            return E_REJECTED;
        case NBD_REP_ERR_BLOCK_SIZE_REQD:
            return E_ARGUMENT;
    }

    return E_FAIL;
}

ui32 NbdErrorCodeToNbsErrorCode(ui32 code)
{
    switch (code) {
        case NBD_SUCCESS:
            return S_OK;
        case NBD_EPERM:
            return E_UNAUTHORIZED;
        case NBD_EIO:
            return E_IO;
        case NBD_ENOMEM:
            return E_INVALID_STATE;
        case NBD_EINVAL:
            return E_ARGUMENT;
        case NBD_ENOSPC:
            return E_INVALID_STATE;
        case NBD_EOVERFLOW:
            return E_ARGUMENT;
        case NBD_ESHUTDOWN:
            return E_REJECTED;
    }

    return E_FAIL;
}

////////////////////////////////////////////////////////////////////////////////

class TStructuredReplyDataReader
{
private:
    TRequestReader& In;
    TStructuredReply& Reply;

    bool ShouldRead = true;

public:
    TStructuredReplyDataReader(TRequestReader& in, TStructuredReply& reply)
        : In(in)
        , Reply(reply)
    {}

    template <typename T>
    size_t ReadData(T& replyData)
    {
        Y_ABORT_UNLESS(ShouldRead);
        ShouldRead = false;

        return In.ReadStructuredReplyData(Reply, replyData);
    }

    void SkipData()
    {
        TBuffer replyData;
        ReadData(replyData);
    }

    void EnsureNoData()
    {
        TBuffer replyData;
        ReadData(replyData);
        Y_ENSURE(!replyData);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TClientHandler final
    : public IClientHandler
{
private:
    const bool StructuredReply;
    const bool UseNbsErrors;

    TLog Log;

    TExportInfo ExportInfo {};
    THashMap<ui64, TClientRequestPtr> RequestsInFlight;

public:
    TClientHandler(
            ILoggingServicePtr logging,
            bool structuredReply,
            bool useNbsErrors)
        : StructuredReply(structuredReply)
        , UseNbsErrors(useNbsErrors)
    {
        Log = logging->CreateLog("BLOCKSTORE_NBD");
    }

    const TExportInfo& GetExportInfo() override
    {
        return ExportInfo;
    }

    bool NegotiateClient(
        IInputStream& in,
        IOutputStream& out) override
    {
        TRequestReader reader(in);
        TRequestWriter writer(out);

        return NegotiateClient(reader, writer);
    }

    void SendRequest(
        IOutputStream& out,
        TClientRequestPtr request) override
    {
        TRequestWriter writer(out);

        SendRequest(writer, std::move(request));
    }

    void ProcessRequests(IInputStream& in) override
    {
        TRequestReader reader(in);

        if (StructuredReply) {
            ProcessRequests_Structured(reader);
        } else {
            ProcessRequests_Simple(reader);
        }
    }

    void CancelAllRequests(const NProto::TError& error) override
    {
        for (auto& it: RequestsInFlight) {
            it.second->Complete(error);
        }

        RequestsInFlight.clear();
    }

private:
    bool NegotiateClient(
        TRequestReader& in,
        TRequestWriter& out);

    bool ProcessOption_StructuredReply(
        TRequestReader& in,
        TRequestWriter& out);

    bool ProcessOption_UseNbsErrors(
        TRequestReader& in,
        TRequestWriter& out);

    bool ProcessOption_Go(
        TRequestReader& in,
        TRequestWriter& out);

    void SendRequest(
        TRequestWriter& out,
        TClientRequestPtr request);

    void ProcessRequests_Simple(TRequestReader& in);
    void ProcessRequests_Structured(TRequestReader& in);

    void CompleteRequest(
        THashMap<ui64, TClientRequestPtr>::iterator& it,
        const NProto::TError& error);

    NProto::TError GetError(ui32 code, TString message);

private:
    void WriteOption(
        TRequestWriter& out,
        ui32 option,
        TStringBuf optionData = {})
    {
        STORAGE_DEBUG(ExportInfo
            << " SEND Option"
            << " option:" << option
            << " length:" << optionData.length());
        out.WriteOption(option, optionData);
    }

    void WriteRequest(
        TRequestWriter& out,
        const TRequest& request,
        const TSgList& requestData = {})
    {
        STORAGE_DEBUG(ExportInfo
            << " SEND Request"
            << " type:" << request.Type
            << " handle:" << request.Handle
            << " from:" << request.From
            << " length:" << request.Length);
        out.WriteRequest(request, requestData);
    }

    bool ReadOptionReply(
        TRequestReader& in,
        TOptionReply& reply,
        TBuffer& replyData)
    {
        if (in.ReadOptionReply(reply, replyData)) {
            STORAGE_DEBUG(ExportInfo
                << " RECEIVE OptionReply"
                << " option:" << reply.Option
                << " type:" << reply.Type
                << " length:" << reply.Length);
            return true;
        }

        return false;
    }

    bool ReadSimpleReply(TRequestReader& in, TSimpleReply& reply)
    {
        if (in.ReadSimpleReply(reply)) {
            STORAGE_DEBUG(ExportInfo
                << " RECEIVE SimpleReply"
                << " #" << reply.Handle
                << " error:" << reply.Error);
            return true;
        }

        return false;
    }

    bool ReadStructuredReply(TRequestReader& in, TStructuredReply& reply)
    {
        if (in.ReadStructuredReply(reply)) {
            STORAGE_DEBUG(ExportInfo
                << " RECEIVE StructuredReply"
                << " #" << reply.Handle
                << " flags:" << reply.Flags
                << " type:" << reply.Type
                << " length:" << reply.Length);
            return true;
        }

        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

bool TClientHandler::NegotiateClient(TRequestReader& in, TRequestWriter& out)
{
    STORAGE_DEBUG(ExportInfo << " Negotiate client connection");

    TServerHello hello;
    if (in.ReadServerHello(hello)) {
        Y_ENSURE(hello.Flags == (NBD_FLAG_FIXED_NEWSTYLE | NBD_FLAG_NO_ZEROES));

        out.WriteClientHello(NBD_FLAG_C_FIXED_NEWSTYLE | NBD_FLAG_C_NO_ZEROES);

        return (!StructuredReply || ProcessOption_StructuredReply(in, out))
            && (!UseNbsErrors || ProcessOption_UseNbsErrors(in, out))
            && ProcessOption_Go(in, out);
    }

    return false;
}

bool TClientHandler::ProcessOption_StructuredReply(
    TRequestReader& in,
    TRequestWriter& out)
{
    WriteOption(out, NBD_OPT_STRUCTURED_REPLY);

    TOptionReply reply;
    TBuffer replyData;

    if (ReadOptionReply(in, reply, replyData)) {
        Y_ENSURE(reply.Option == NBD_OPT_STRUCTURED_REPLY);

        switch (reply.Type) {
            case NBD_REP_ACK:
                Y_ENSURE(!replyData.Size());
                return true;

            default: {
                auto errorCode = OptionReplyTypeToProtoErrorCode(reply.Type);
                ythrow TServiceError(errorCode)
                    << "request failed with error: " << AsStringBuf(replyData);
            }
        }
    }

    return false;
}

bool TClientHandler::ProcessOption_UseNbsErrors(
    TRequestReader& in,
    TRequestWriter& out)
{
    WriteOption(out, NBD_OPT_USE_NBS_ERRORS, NBS_OPTION_TAG);

    TOptionReply reply;
    TBuffer replyData;

    if (ReadOptionReply(in, reply, replyData)) {
        Y_ENSURE(reply.Option == NBD_OPT_USE_NBS_ERRORS);

        switch (reply.Type) {
            case NBD_REP_ACK:
                Y_ENSURE(!replyData.Size());
                return true;

            default: {
                auto errorCode = OptionReplyTypeToProtoErrorCode(reply.Type);
                ythrow TServiceError(errorCode)
                    << "request failed with error: " << AsStringBuf(replyData);
            }
        }
    }

    return false;
}

bool TClientHandler::ProcessOption_Go(
    TRequestReader& in,
    TRequestWriter& out)
{
    TExportInfoRequest request;
    request.InfoTypes = { NBD_INFO_NAME, NBD_INFO_EXPORT, NBD_INFO_BLOCK_SIZE };

    TBufferRequestWriter requestOut;
    requestOut.WriteExportInfoRequest(request);

    WriteOption(out, NBD_OPT_GO, AsStringBuf(requestOut.Buffer()));

    TSet<ui16> infoTypes;

    TOptionReply reply;
    TBuffer replyData;

    while (ReadOptionReply(in, reply, replyData)) {
        Y_ENSURE(reply.Option == NBD_OPT_GO);

        switch (reply.Type) {
            case NBD_REP_INFO: {
                TBufferRequestReader replyIn(replyData);

                ui16 type;
                Y_ENSURE(replyIn.ReadExportInfo(ExportInfo, type));

                infoTypes.insert(type);
                break;
            }

            case NBD_REP_ACK: {
                Y_ENSURE(!replyData.Size());

                for (auto type: request.InfoTypes) {
                    if (!infoTypes.contains(type)) {
                        return false;
                    }
                }

                return true;
            }

            default: {
                auto errorCode = OptionReplyTypeToProtoErrorCode(reply.Type);
                ythrow TServiceError(errorCode)
                    << "request failed with error: " << AsStringBuf(replyData);
            }
        }
    }

    return false;
}

void TClientHandler::SendRequest(
    TRequestWriter& out,
    TClientRequestPtr request)
{
    ui64 requestId = request->RequestId;
    if (!requestId) {
        requestId = CreateRequestId();
    }

    TRequest req = {};
    req.Magic = NBD_REQUEST_MAGIC;
    req.Handle = requestId;
    req.From = request->BlockIndex * GetBlockSize(ExportInfo);
    req.Length = request->BlocksCount * GetBlockSize(ExportInfo);

    switch (request->RequestType) {
        case EClientRequestType::ReadBlocks: {
            req.Type = NBD_CMD_READ;
            WriteRequest(out, req);
            break;
        }

        case EClientRequestType::WriteBlocks: {
            auto guard = request->SgList.Acquire();
            if (!guard) {
                request->Complete(TErrorResponse(
                    E_CANCELLED,
                    "failed to acquire sglist in NbdClient"));
                return;
            }

            req.Type = NBD_CMD_WRITE;
            WriteRequest(out, req, guard.Get());
            break;
        }

        case EClientRequestType::ZeroBlocks: {
            req.Type = NBD_CMD_WRITE_ZEROES;
            WriteRequest(out, req);
            break;
        }

        case EClientRequestType::MountVolume: {
            STORAGE_DEBUG(ExportInfo << " Connected to server");
            request->Complete({});
            return;
        }
    }

    auto [it, inserted] = RequestsInFlight.emplace(
        requestId,
        std::move(request));
    Y_ABORT_UNLESS(inserted);
}

void TClientHandler::ProcessRequests_Simple(TRequestReader& in)
{
    TSimpleReply reply;

    while (ReadSimpleReply(in, reply)) {
        auto it = RequestsInFlight.find(reply.Handle);
        if (it == RequestsInFlight.end()) {
            STORAGE_ERROR(ExportInfo
                << " invalid reply handle: " << reply.Handle);
            continue;
        }

        TClientRequest* request = it->second.Get();

        if (reply.Error) {
            auto error = GetError(reply.Error, "request failed");
            CompleteRequest(it, error);
            continue;
        }

        if (request->RequestType == EClientRequestType::ReadBlocks) {
            size_t length = request->BlocksCount * GetBlockSize(ExportInfo);

            auto guard = request->SgList.Acquire();
            if (!guard) {
                TBuffer replyData;
                in.ReadOrFail(replyData, length);

                CompleteRequest(it, TErrorResponse(
                    E_CANCELLED,
                    "failed to acquire sglist in NbdClient"));
                continue;
            }

            in.ReadOrFail(guard.Get(), length);
        }

        CompleteRequest(it, {});
    }
}

void TClientHandler::ProcessRequests_Structured(TRequestReader& in)
{
    TStructuredReply reply;

    while (ReadStructuredReply(in, reply)) {
        TStructuredReplyDataReader replyDataReader(in, reply);

        auto it = RequestsInFlight.find(reply.Handle);
        if (it == RequestsInFlight.end()) {
            STORAGE_ERROR(ExportInfo
                << " invalid reply handle: " << reply.Handle);

            replyDataReader.SkipData();
            continue;
        }

        TClientRequest* request = it->second.Get();

        if (ReplyTypeIsError(reply.Type)) {
            TBuffer replyData;
            replyDataReader.ReadData(replyData);
            auto error = GetError(reply.Error.Error, TStringBuilder()
                << "request failed with error: " << AsStringBuf(replyData));

            CompleteRequest(it, error);
            continue;
        }

        switch (request->RequestType) {
            case EClientRequestType::ReadBlocks: {
                Y_ENSURE(reply.Type == NBD_REPLY_TYPE_OFFSET_DATA);
                Y_ENSURE(reply.Flags & NBD_REPLY_FLAG_DONE);

                auto guard = request->SgList.Acquire();
                if (!guard) {
                    CompleteRequest(it, TErrorResponse(
                        E_CANCELLED,
                        "failed to acquire sglist in NbdClient"));

                    replyDataReader.SkipData();
                    continue;
                }

                auto replyDataSize = replyDataReader.ReadData(guard.Get());

                size_t length = request->BlocksCount * GetBlockSize(ExportInfo);
                Y_ENSURE(replyDataSize == length);
                break;
            }

            case EClientRequestType::WriteBlocks:
            case EClientRequestType::ZeroBlocks: {
                Y_ENSURE(reply.Type == NBD_REPLY_TYPE_NONE);
                Y_ENSURE(reply.Flags & NBD_REPLY_FLAG_DONE);

                replyDataReader.EnsureNoData();
                break;
            }
            case EClientRequestType::MountVolume: {
                Y_ABORT("MountVolume request should not get receive");
            }
        }

        CompleteRequest(it, {});
    }
}

void TClientHandler::CompleteRequest(
    THashMap<ui64, TClientRequestPtr>::iterator& it,
    const NProto::TError& error)
{
    TClientRequestPtr request = std::move(it->second);
    RequestsInFlight.erase(it);
    request->Complete(error);
}

NProto::TError TClientHandler::GetError(ui32 code, TString message)
{
    if (!UseNbsErrors) {
        code = NbdErrorCodeToNbsErrorCode(code);
    }

    NProto::TError error;
    error.SetCode(code);
    error.SetMessage(std::move(message));
    return error;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TClientRequest::TClientRequest(
        EClientRequestType requestType,
        ui64 requestId,
        ui64 blockIndex,
        ui32 blocksCount,
        TGuardedSgList sglist)
    : RequestType(requestType)
    , RequestId(requestId)
    , BlockIndex(blockIndex)
    , BlocksCount(blocksCount)
    , SgList(std::move(sglist))
{}

////////////////////////////////////////////////////////////////////////////////

IClientHandlerPtr CreateClientHandler(
    ILoggingServicePtr logging,
    bool structuredReply,
    bool useNbsErrors)
{
    return std::make_shared<TClientHandler>(
        std::move(logging),
        structuredReply,
        useNbsErrors);
}

////////////////////////////////////////////////////////////////////////////////

IOutputStream& operator <<(IOutputStream& out, const TExportInfo& info)
{
    if (info.Name) {
        out << "[d:" << info.Name << "] ";
    }

    return out;
}

}   // namespace NCloud::NBlockStore::NBD
