#include "request.h"

#include "endpoint.h"
#include "filestore.h"

#include <google/protobuf/message.h>
#include <google/protobuf/text_format.h>

#include <util/generic/singleton.h>
#include <util/random/random.h>
#include <util/string/builder.h>

namespace NCloud::NFileStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TSizePrinter
    : google::protobuf::TextFormat::FastFieldValuePrinter
{
    void PrintString(
        const google::protobuf::string& val,
        google::protobuf::TextFormat::BaseTextGenerator* generator) const override
    {
        generator->PrintString(TStringBuilder() << val.size() << " bytes");
    }

    void PrintBytes(
        const google::protobuf::string& val,
        google::protobuf::TextFormat::BaseTextGenerator* generator) const override
    {
        generator->PrintString(TStringBuilder() << val.size() << " bytes");
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IOutputStream& operator <<(IOutputStream& out, const TRequestInfo& info)
{
    if (info.FileSystemId) {
        out << "[f:" << info.FileSystemId << "] ";
    }

    if (info.SessionId) {
        out << "[s:" << info.SessionId << "] ";
    }

    if (info.ClientId) {
        out << "[c:" << info.ClientId << "] ";
    }

    out << info.RequestName;

    if (info.RequestId) {
        out << " #" << info.RequestId;
    }

    return out;
}

////////////////////////////////////////////////////////////////////////////////

ui64 CreateRequestId()
{
    ui64 requestId = 0;
    while (!requestId) {
        requestId = RandomNumber<ui64>();
    }

    return requestId;
}

////////////////////////////////////////////////////////////////////////////////

TProtoMessagePrinter::TProtoMessagePrinter()
{
    Printer.SetSingleLineMode(true);
    Printer.SetExpandAny(true);
    Printer.SetTruncateStringFieldLongerThan(256);

    Printer.RegisterFieldValuePrinter(
        NProto::TWriteDataRequest::descriptor()->FindFieldByLowercaseName("buffer"),
        new TSizePrinter());

    Printer.RegisterFieldValuePrinter(
        NProto::TReadDataResponse::descriptor()->FindFieldByLowercaseName("buffer"),
        new TSizePrinter());
}

TString TProtoMessagePrinter::ToString(
    const google::protobuf::Message& message)
{
    TString result;
    Printer.PrintToString(message, &result);

    // single line mode currently might have an extra space at the end
    if (result.size() > 0 && result.back() == ' ') {
        result.pop_back();
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

template <>
ui64 CalculateByteCount<NProto::TReadDataRequest>(
    const NProto::TReadDataRequest& request)
{
    return request.GetLength();
}

template <>
ui64 CalculateByteCount<NProto::TWriteDataRequest>(
    const NProto::TWriteDataRequest& request)
{
    if (!request.GetBuffer().empty()) {
        return request.GetBuffer().size();
    }

    ui64 byteCount = 0;
    for (const auto& iovec: request.GetIovecs()) {
        byteCount += iovec.GetLength();
    }
    return byteCount;
}

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_DECLARE_REQUEST(name, ...) #name,

static const TString RequestNames[] = {
    FILESTORE_REQUESTS(FILESTORE_DECLARE_REQUEST)
    "DescribeData",
    "GenerateBlobIds",
    "AddData",
    "ReadBlob",
    "WriteBlob",
    "ConfirmAddData",
    "CancelAddData",
};

static_assert(
    sizeof(RequestNames) / sizeof(RequestNames[0]) == FileStoreRequestCount,
    "RequestNames size mismatch");

#undef FILESTORE_DECLARE_REQUEST

const TString& GetFileStoreRequestName(EFileStoreRequest requestType)
{
    size_t index = static_cast<size_t>(requestType);
    if (index < FileStoreRequestCount) {
        return RequestNames[index];
    }

    static const TString unknown = "Unknown";
    return unknown;
}

}   // namespace NCloud::NFileStore
