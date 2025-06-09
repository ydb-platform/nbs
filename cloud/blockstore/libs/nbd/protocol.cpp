#include "protocol.h"

namespace NCloud::NBlockStore::NBD {

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
size_t ReadStructuredReplyDataImpl(
    TBinaryReader& in,
    TStructuredReply& reply,
    T& replyData)
{
    size_t replyDataSize;

    switch (reply.Type) {
        case NBD_REPLY_TYPE_NONE:
            Y_ENSURE(reply.Length == 0);

            replyDataSize = 0;
            break;

        case NBD_REPLY_TYPE_OFFSET_DATA:
            Y_ENSURE(reply.Length <= NBD_MAX_BUFFER_SIZE);
            Y_ENSURE(reply.Length >= sizeof(TStructuredReadData));

            in.ReadOrFail(reply.ReadData.Offset);
            replyDataSize = reply.Length - sizeof(TStructuredReadData);
            break;

        case NBD_REPLY_TYPE_OFFSET_HOLE:
            Y_ENSURE(reply.Length == sizeof(TStructuredReadHole));

            in.ReadOrFail(reply.ReadHole.Offset);
            in.ReadOrFail(reply.ReadHole.DataLength);

            replyDataSize = 0;
            break;

        case NBD_REPLY_TYPE_ERROR:
            Y_ENSURE(reply.Length <= NBD_MAX_BUFFER_SIZE);
            Y_ENSURE(reply.Length >= sizeof(TStructuredError));

            in.ReadOrFail(reply.Error.Error);
            in.ReadOrFail(reply.Error.MessageLength);

            Y_ENSURE(reply.Error.MessageLength == reply.Length - sizeof(TStructuredError));
            replyDataSize = reply.Error.MessageLength;
            break;

        default:
            ythrow yexception()
                << "unsupported reply type: " << reply.Type;
    }

    in.ReadOrFail(replyData, replyDataSize);
    return replyDataSize;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TRequestWriter::WriteServerHello(ui16 flags)
{
    Write<ui64>(NBD_MAGIC);
    Write<ui64>(NBD_OPTS_MAGIC);
    Write<ui16>(flags);

    Flush();
}

void TRequestWriter::WriteClientHello(ui32 flags)
{
    Write<ui32>(flags);

    Flush();
}

void TRequestWriter::WriteOption(ui32 option, TStringBuf optionData)
{
    Y_DEBUG_ABORT_UNLESS(optionData.size() <= NBD_MAX_OPTION_SIZE);

    Write<ui64>(NBD_OPTS_MAGIC);
    Write<ui32>(option);
    Write<ui32>(optionData.size());
    Write(optionData);

    Flush();
}

void TRequestWriter::WriteExportInfoRequest(const TExportInfoRequest& request)
{
    Y_DEBUG_ABORT_UNLESS(request.Name.size() <= NBD_MAX_NAME_SIZE);
    Y_DEBUG_ABORT_UNLESS(request.InfoTypes.size() <= NBD_MAX_NAME_SIZE);

    Write<ui32>(request.Name.size());
    Write(request.Name);
    Write<ui16>(request.InfoTypes.size());

    for (ui16 type: request.InfoTypes) {
        Write<ui16>(type);
    }

    Flush();
}

void TRequestWriter::WriteExportList(const TExportInfo& exp)
{
    Y_DEBUG_ABORT_UNLESS(exp.Name.size() <= NBD_MAX_NAME_SIZE);

    Write<ui32>(exp.Name.size());
    Write(exp.Name);

    Flush();
}

void TRequestWriter::WriteExportInfo(const TExportInfo& exp, ui16 type)
{
    Write<ui16>(type);

    switch (type) {
        case NBD_INFO_EXPORT:
            Write<ui64>(exp.Size);
            Write<ui16>(exp.Flags);
            break;

        case NBD_INFO_NAME:
            Write(exp.Name);
            break;

        case NBD_INFO_BLOCK_SIZE:
            Write<ui32>(exp.MinBlockSize);
            Write<ui32>(exp.OptBlockSize);
            Write<ui32>(exp.MaxBlockSize);
            break;

        default:
            Y_ABORT();
    }

    Flush();
}

void TRequestWriter::WriteOptionReply(
    ui32 option,
    ui32 type,
    TStringBuf replyData)
{
    Y_DEBUG_ABORT_UNLESS(replyData.size() <= NBD_MAX_OPTION_SIZE);

    Write<ui64>(NBD_REP_MAGIC);
    Write<ui32>(option);
    Write<ui32>(type);
    Write<ui32>(replyData.size());
    Write(replyData);

    Flush();
}

void TRequestWriter::WriteRequest(
    const TRequest& request,
    TStringBuf requestData)
{
    Y_DEBUG_ABORT_UNLESS(request.Magic == NBD_REQUEST_MAGIC);

    if (request.Type != NBD_CMD_TRIM && request.Type != NBD_CMD_WRITE_ZEROES) {
        Y_DEBUG_ABORT_UNLESS(request.Length <= NBD_MAX_BUFFER_SIZE);
    }

    Write<ui32>(request.Magic);
    Write<ui16>(request.Flags);
    Write<ui16>(request.Type);
    Write<ui64>(request.Handle);
    Write<ui64>(request.From);
    Write<ui32>(request.Length);

    if (request.Type == NBD_CMD_WRITE) {
        Y_DEBUG_ABORT_UNLESS(requestData.size() == request.Length);
        Write(requestData);
    } else {
        Y_DEBUG_ABORT_UNLESS(!requestData);
    }

    Flush();
}

void TRequestWriter::WriteRequest(
    const TRequest& request,
    const TSgList& requestData)
{
    Y_DEBUG_ABORT_UNLESS(request.Magic == NBD_REQUEST_MAGIC);
    if (request.Type != NBD_CMD_WRITE_ZEROES) {
        Y_DEBUG_ABORT_UNLESS(request.Length <= NBD_MAX_BUFFER_SIZE);
    }

    Write<ui32>(request.Magic);
    Write<ui16>(request.Flags);
    Write<ui16>(request.Type);
    Write<ui64>(request.Handle);
    Write<ui64>(request.From);
    Write<ui32>(request.Length);

    if (request.Type == NBD_CMD_WRITE) {
        size_t length = Write(requestData);
        Y_DEBUG_ABORT_UNLESS(length == request.Length);
    } else {
        Y_DEBUG_ABORT_UNLESS(!requestData);
    }

    Flush();
}

void TRequestWriter::WriteSimpleReply(ui64 handle, ui32 error)
{
    Write<ui32>(NBD_SIMPLE_REPLY_MAGIC);
    Write<ui32>(error);
    Write<ui64>(handle);

    Flush();
}

void TRequestWriter::WriteStructuredDone(ui64 handle)
{
    Write<ui32>(NBD_STRUCTURED_REPLY_MAGIC);
    Write<ui16>(NBD_REPLY_FLAG_DONE);
    Write<ui16>(NBD_REPLY_TYPE_NONE);
    Write<ui64>(handle);
    Write<ui32>(0);

    Flush();
}

void TRequestWriter::WriteStructuredReadData(
    ui64 handle,
    ui64 offset,
    ui32 length,
    bool final)
{
    Y_DEBUG_ABORT_UNLESS(length <= NBD_MAX_BUFFER_SIZE);

    Write<ui32>(NBD_STRUCTURED_REPLY_MAGIC);
    Write<ui16>(final ? NBD_REPLY_FLAG_DONE : NBD_REPLY_FLAG_NONE);
    Write<ui16>(NBD_REPLY_TYPE_OFFSET_DATA);
    Write<ui64>(handle);
    Write<ui32>(sizeof(TStructuredReadData) + length);

    Write<ui64>(offset);

    Flush();
}

void TRequestWriter::WriteStructuredReadHole(
    ui64 handle,
    ui64 offset,
    ui32 length,
    bool final)
{
    Y_DEBUG_ABORT_UNLESS(length <= NBD_MAX_BUFFER_SIZE);

    Write<ui32>(NBD_STRUCTURED_REPLY_MAGIC);
    Write<ui16>(final ? NBD_REPLY_FLAG_DONE : NBD_REPLY_FLAG_NONE);
    Write<ui16>(NBD_REPLY_TYPE_OFFSET_HOLE);
    Write<ui64>(handle);
    Write<ui32>(sizeof(TStructuredReadHole));

    Write<ui64>(offset);
    Write<ui32>(length);

    Flush();
}

void TRequestWriter::WriteStructuredError(
    ui64 handle,
    ui32 error,
    TStringBuf message)
{
    Y_DEBUG_ABORT_UNLESS(message.size() <= NBD_MAX_BUFFER_SIZE);

    Write<ui32>(NBD_STRUCTURED_REPLY_MAGIC);
    Write<ui16>(NBD_REPLY_FLAG_DONE);
    Write<ui16>(NBD_REPLY_TYPE_ERROR);
    Write<ui64>(handle);
    Write<ui32>(sizeof(TStructuredError) + message.size());

    Write<ui32>(error);
    Write<ui16>(message.size());
    Write(message);

    Flush();
}

////////////////////////////////////////////////////////////////////////////////

bool TRequestReader::ReadServerHello(TServerHello& hello)
{
    if (Read(hello.Passwd)) {
        Y_ENSURE(hello.Passwd == NBD_MAGIC);

        ReadOrFail(hello.Magic);
        Y_ENSURE(hello.Magic == NBD_OPTS_MAGIC);

        ReadOrFail(hello.Flags);
        return true;
    }

    return false;
}

bool TRequestReader::ReadClientHello(TClientHello& hello)
{
    return Read(hello.Flags);
}

bool TRequestReader::ReadOption(TOption& option, TBuffer& optionData)
{
    if (Read(option.Magic)) {
        Y_ENSURE(option.Magic == NBD_OPTS_MAGIC);

        ReadOrFail(option.Option);
        ReadOrFail(option.Length);

        Y_ENSURE(option.Length <= NBD_MAX_OPTION_SIZE);
        ReadOrFail(optionData, option.Length);
        return true;
    }

    return false;
}

bool TRequestReader::ReadExportInfoRequest(TExportInfoRequest& request)
{
    ui32 len;
    if (Read(len)) {
        Y_ENSURE(len <= NBD_MAX_NAME_SIZE);
        ReadOrFail(request.Name, len);

        ui16 count;
        ReadOrFail(count);

        Y_ENSURE(count <= NBD_MAX_NAME_SIZE);
        request.InfoTypes.clear();
        request.InfoTypes.reserve(count);

        ui16 type;
        while (count--) {
            ReadOrFail(type);
            request.InfoTypes.push_back(type);
        }

        return true;
    }

    return false;
}

bool TRequestReader::ReadExportList(TExportInfo& exp)
{
    ui32 len;
    if (Read(len)) {
        Y_ENSURE(len <= NBD_MAX_NAME_SIZE);
        ReadOrFail(exp.Name, len);
        return true;
    }

    return false;
}

bool TRequestReader::ReadExportInfo(TExportInfo& exp, ui16& type)
{
    if (Read(type)) {
        switch (type) {
            case NBD_INFO_EXPORT:
                ReadOrFail(exp.Size);
                ReadOrFail(exp.Flags);
                break;

            case NBD_INFO_NAME:
                // TODO
                exp.Name = In.ReadAll();
                break;

            case NBD_INFO_BLOCK_SIZE:
                ReadOrFail(exp.MinBlockSize);
                ReadOrFail(exp.OptBlockSize);
                ReadOrFail(exp.MaxBlockSize);
                break;

            default:
                // just ignore unknown info
                break;
        }

        return true;
    }

    return false;
}

bool TRequestReader::ReadOptionReply(TOptionReply& reply, TBuffer& replyData)
{
    if (Read(reply.Magic)) {
        Y_ENSURE(reply.Magic == NBD_REP_MAGIC);

        ReadOrFail(reply.Option);
        ReadOrFail(reply.Type);
        ReadOrFail(reply.Length);

        Y_ENSURE(reply.Length <= NBD_MAX_OPTION_SIZE);
        ReadOrFail(replyData, reply.Length);
        return true;
    }

    return false;
}

bool TRequestReader::ReadRequest(TRequest& request)
{
    if (Read(request.Magic)) {
        Y_ENSURE(request.Magic == NBD_REQUEST_MAGIC);

        ReadOrFail(request.Flags);
        ReadOrFail(request.Type);
        ReadOrFail(request.Handle);
        ReadOrFail(request.From);
        ReadOrFail(request.Length);

        if (request.Type != NBD_CMD_TRIM &&
            request.Type != NBD_CMD_WRITE_ZEROES)
        {
            Y_ENSURE(request.Length <= NBD_MAX_BUFFER_SIZE);
        }

        return true;
    }

    return false;
}

bool TRequestReader::ReadSimpleReply(TSimpleReply& reply)
{
    if (Read(reply.Magic)) {
        Y_ENSURE(reply.Magic == NBD_SIMPLE_REPLY_MAGIC);

        ReadOrFail(reply.Error);
        ReadOrFail(reply.Handle);

        return true;
    }

    return false;
}

bool TRequestReader::ReadStructuredReply(TStructuredReply& reply)
{
    if (Read(reply.Magic)) {
        Y_ENSURE(reply.Magic == NBD_STRUCTURED_REPLY_MAGIC);

        ReadOrFail(reply.Flags);
        ReadOrFail(reply.Type);
        ReadOrFail(reply.Handle);
        ReadOrFail(reply.Length);
        return true;
    }

    return false;
}

size_t TRequestReader::ReadStructuredReplyData(
    TStructuredReply& reply,
    TBuffer& replyData)
{
    return ReadStructuredReplyDataImpl(*this, reply, replyData);
}

size_t TRequestReader::ReadStructuredReplyData(
    TStructuredReply& reply,
    const TSgList& replyData)
{
    return ReadStructuredReplyDataImpl(*this, reply, replyData);
}

}   // namespace NCloud::NBlockStore::NBD
