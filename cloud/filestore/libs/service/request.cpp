#include "request.h"

#include "endpoint.h"
#include "filestore.h"

#include <util/generic/singleton.h>
#include <util/random/random.h>
#include <util/string/builder.h>

#include <google/protobuf/message.h>
#include <google/protobuf/text_format.h>

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

////////////////////////////////////////////////////////////////////////////////

struct TTextPrinter
    : google::protobuf::TextFormat::Printer
{
    TTextPrinter()
    {
        SetSingleLineMode(true);
        SetExpandAny(true);
        SetTruncateStringFieldLongerThan(256);

        RegisterFieldValuePrinter(
            NProto::TWriteDataRequest::descriptor()->FindFieldByLowercaseName("buffer"),
            new TSizePrinter());

        RegisterFieldValuePrinter(
            NProto::TReadDataResponse::descriptor()->FindFieldByLowercaseName("buffer"),
            new TSizePrinter());
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

TString DumpMessage(const google::protobuf::Message& message)
{
    TString result;
    Singleton<TTextPrinter>()->PrintToString(message, &result);

    // single line mode currently might have an extra space at the end
    if (result.size() > 0 && result.back() == ' ') {
        result.pop_back();
    }

    return result;
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

#define FILESTORE_DECLARE_REQUEST(name, ...) #name,

static const TString RequestNames[] = {
    FILESTORE_REQUESTS(FILESTORE_DECLARE_REQUEST)
    "DescribeData",
    "GenerateBlobIds",
    "AddData",
    "ReadBlob",
    "WriteBlob",
    "ReadNodeRefs",
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
