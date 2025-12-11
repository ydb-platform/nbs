#include "client_handler.h"

#include "protocol.h"
#include "utils.h"

#include <cloud/blockstore/libs/service/context.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/str.h>

namespace NCloud::NBlockStore::NBD {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestClientRequest final: public TClientRequest
{
    using TClientRequest::TClientRequest;

    TPromise<NProto::TError> Response = NewPromise<NProto::TError>();

    void Complete(const NProto::TError& error) override
    {
        Response.SetValue(error);
    }
};

////////////////////////////////////////////////////////////////////////////////

void WriteReadReply(
    TRequestWriter& out,
    bool structuredReply,
    ui64 handle,
    ui64 blockIndex,
    ui32 blocksCount)
{
    if (structuredReply) {
        out.WriteStructuredReadData(
            handle,
            blockIndex,
            blocksCount * DefaultBlockSize,
            true);
    } else {
        out.WriteSimpleReply(handle, 0);
    }
}

void WriteModifyReply(TRequestWriter& out, bool structuredReply, ui64 handle)
{
    if (structuredReply) {
        out.WriteStructuredDone(handle);
    } else {
        out.WriteSimpleReply(handle, 0);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
private:
    ILoggingServicePtr Logging;

public:
    TBootstrap(ILoggingServicePtr logging)
        : Logging(std::move(logging))
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
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TBootstrap> CreateBootstrap()
{
    return std::make_unique<TBootstrap>(
        CreateLoggingService("console", {TLOG_DEBUG}));
}

void NegotiateClient(
    IClientHandler& handler,
    TStringStream& in,
    TStringStream& out,
    bool structuredReply)
{
    TRequestReader reader(in);
    TRequestWriter writer(out);

    writer.WriteServerHello(NBD_FLAG_FIXED_NEWSTYLE | NBD_FLAG_NO_ZEROES);

    if (structuredReply) {
        writer.WriteOptionReply(NBD_OPT_STRUCTURED_REPLY, NBD_REP_ACK);
    }

    {
        TExportInfo exp{};
        exp.Name = "test";
        exp.Size = 4 * 1024 * 1024 * 1024ull;
        exp.MinBlockSize = DefaultBlockSize;
        exp.OptBlockSize = DefaultBlockSize;
        exp.MaxBlockSize = NBD_MAX_BUFFER_SIZE;

        exp.Flags = NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_TRIM |
                    NBD_FLAG_SEND_WRITE_ZEROES;

        for (ui16 type: {NBD_INFO_BLOCK_SIZE, NBD_INFO_EXPORT, NBD_INFO_NAME}) {
            TBufferRequestWriter response;
            response.WriteExportInfo(exp, type);

            writer.WriteOptionReply(
                NBD_OPT_GO,
                NBD_REP_INFO,
                AsStringBuf(response.Buffer()));
        }

        writer.WriteOptionReply(NBD_OPT_GO, NBD_REP_ACK);
    }

    UNIT_ASSERT(handler.NegotiateClient(out, in));
}

void ProcessRequests(
    IClientHandler& handler,
    TStringStream& in,
    TStringStream& out,
    bool structuredReply)
{
    TRequestReader reader(in);
    TRequestWriter writer(out);

    constexpr ui64 blockIndex = 1;
    constexpr ui32 blocksCount = 1;

    // send ReadBlocks
    TGuardedBuffer readData(TString(DefaultBlockSize, ' '));
    auto readSglist = readData.GetGuardedSgList();

    ui64 readRequestId = 1;
    auto readRequest = MakeIntrusive<TTestClientRequest>(
        EClientRequestType::ReadBlocks,
        readRequestId,
        blockIndex,
        blocksCount,
        std::move(readSglist));
    handler.SendRequest(in, readRequest);

    WriteReadReply(
        writer,
        structuredReply,
        readRequestId,
        readRequest->BlockIndex,
        readRequest->BlocksCount);

    TString readDataResponse(DefaultBlockSize, 'a');
    writer.Write(readDataResponse);

    // send WriteBlocks
    TGuardedBuffer writeData(TString(DefaultBlockSize, 'b'));
    auto writeSglist = writeData.GetGuardedSgList();

    ui64 writeRequestId = 2;
    auto writeRequest = MakeIntrusive<TTestClientRequest>(
        EClientRequestType::WriteBlocks,
        writeRequestId,
        blockIndex,
        blocksCount,
        std::move(writeSglist));
    handler.SendRequest(in, writeRequest);

    WriteModifyReply(writer, structuredReply, writeRequestId);

    // send ZeroBlocks
    ui64 zeroRequestId = 3;
    auto zeroRequest = MakeIntrusive<TTestClientRequest>(
        EClientRequestType::ZeroBlocks,
        zeroRequestId,
        blockIndex,
        blocksCount);
    handler.SendRequest(in, zeroRequest);

    WriteModifyReply(writer, structuredReply, zeroRequestId);

    // execute processing
    handler.ProcessRequests(out);

    UNIT_ASSERT(readRequest->Response.HasValue());
    UNIT_ASSERT(writeRequest->Response.HasValue());
    UNIT_ASSERT(zeroRequest->Response.HasValue());

    auto readError = readRequest->Response.GetValue();
    auto writeError = writeRequest->Response.GetValue();
    auto zeroError = zeroRequest->Response.GetValue();

    UNIT_ASSERT_C(!HasError(readError), readError);
    UNIT_ASSERT_C(!HasError(writeError), writeError);
    UNIT_ASSERT_C(!HasError(zeroError), zeroError);

    UNIT_ASSERT_VALUES_EQUAL(readData.Get(), readDataResponse);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TClientHandlerTest)
{
    void ShouldNegotiateClient(bool structuredReply)
    {
        auto bootstrap = CreateBootstrap();
        bootstrap->Start();

        auto handler =
            CreateClientHandler(bootstrap->GetLogging(), structuredReply);

        TStringStream in;
        TStringStream out;

        NegotiateClient(*handler, in, out, structuredReply);

        bootstrap->Stop();
    }

    Y_UNIT_TEST(ShouldNegotiateClient_Simple)
    {
        ShouldNegotiateClient(false);
    }

    Y_UNIT_TEST(ShouldNegotiateClient_Structured)
    {
        ShouldNegotiateClient(true);
    }

    void ShouldSendRequests(bool structuredReply)
    {
        auto bootstrap = CreateBootstrap();
        bootstrap->Start();

        auto handler =
            CreateClientHandler(bootstrap->GetLogging(), structuredReply);

        TStringStream in;
        TStringStream out;

        NegotiateClient(*handler, in, out, structuredReply);
        ProcessRequests(*handler, in, out, structuredReply);

        bootstrap->Stop();
    }

    Y_UNIT_TEST(ShouldSendRequests_Simple)
    {
        ShouldSendRequests(false);
    }

    Y_UNIT_TEST(ShouldSendRequests_Structured)
    {
        ShouldSendRequests(true);
    }

    void ShouldHandleIncompleteReadResponse(bool structuredReply)
    {
        auto bootstrap = CreateBootstrap();
        bootstrap->Start();

        auto handler =
            CreateClientHandler(bootstrap->GetLogging(), structuredReply);

        TStringStream in;
        TStringStream out;

        NegotiateClient(*handler, in, out, structuredReply);

        TRequestReader reader(in);
        TRequestWriter writer(out);

        constexpr ui64 blockIndex = 1;
        constexpr ui32 blocksCount = 1;

        TGuardedBuffer readData(TString(DefaultBlockSize, ' '));
        auto readSglist = readData.GetGuardedSgList();

        ui64 readRequestId = 1;
        auto readRequest = MakeIntrusive<TTestClientRequest>(
            EClientRequestType::ReadBlocks,
            readRequestId,
            blockIndex,
            blocksCount,
            std::move(readSglist));
        handler->SendRequest(in, readRequest);

        WriteReadReply(
            writer,
            structuredReply,
            readRequestId,
            readRequest->BlockIndex,
            readRequest->BlocksCount);

        TString incompleteReadData(DefaultBlockSize / 2, 'a');
        writer.Write(incompleteReadData);

        bool isException = false;
        try {
            // execute processing
            handler->ProcessRequests(out);
        } catch (...) {
            isException = true;
        }

        UNIT_ASSERT(isException);
        UNIT_ASSERT(!readRequest->Response.HasValue());

        NProto::TError cancelError;
        cancelError.SetCode(E_REJECTED);
        cancelError.SetMessage("Test error message");
        handler->CancelAllRequests(cancelError);

        UNIT_ASSERT(readRequest->Response.HasValue());
        auto response = readRequest->Response.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(cancelError.GetCode(), response.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            cancelError.GetMessage(),
            response.GetMessage());

        bootstrap->Stop();
    }

    Y_UNIT_TEST(ShouldHandleIncompleteReadResponse_Simple)
    {
        ShouldHandleIncompleteReadResponse(false);
    }

    Y_UNIT_TEST(ShouldHandleIncompleteReadResponse_Structured)
    {
        ShouldHandleIncompleteReadResponse(true);
    }
}

}   // namespace NCloud::NBlockStore::NBD
