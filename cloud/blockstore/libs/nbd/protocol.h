#pragma once

#include "public.h"

#include "binary_reader.h"
#include "binary_writer.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/buffer.h>

namespace NCloud::NBlockStore::NBD {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 NBD_MAGIC = 0x4e42444d41474943LL;
constexpr ui64 NBD_OPTS_MAGIC = 0x49484156454f5054LL;
constexpr ui64 NBD_REP_MAGIC = 0x0003e889045565a9LL;

constexpr ui32 NBD_REQUEST_MAGIC = 0x25609513;
constexpr ui32 NBD_SIMPLE_REPLY_MAGIC = 0x67446698;
constexpr ui32 NBD_STRUCTURED_REPLY_MAGIC = 0x668e33ef;

constexpr ui32 NBD_MAX_OPTION_SIZE = 4 * 1024;
constexpr ui32 NBD_MAX_BUFFER_SIZE = 32 * 1024 * 1024;
constexpr ui32 NBD_MAX_NAME_SIZE = 256;

static const TString NBS_OPTION_TAG = "NBS option";

////////////////////////////////////////////////////////////////////////////////
// New-style handshake (global) flags

enum
{
    NBD_FLAG_FIXED_NEWSTYLE = 1 << 0,   // Fixed newstyle protocol
    NBD_FLAG_NO_ZEROES = 1 << 1,        // End handshake without zeroes
};

////////////////////////////////////////////////////////////////////////////////
// New-style client flags

enum
{
    NBD_FLAG_C_FIXED_NEWSTYLE = 1 << 0,   // Fixed newstyle protocol
    NBD_FLAG_C_NO_ZEROES = 1 << 1,        // End handshake without zeroes
};

////////////////////////////////////////////////////////////////////////////////
// Transmission (export) flags

enum
{
    NBD_FLAG_HAS_FLAGS = 1 << 0,    // Flags are there
    NBD_FLAG_READ_ONLY = 1 << 1,    // Device is read-only
    NBD_FLAG_SEND_FLUSH = 1 << 2,   // Send FLUSH
    NBD_FLAG_SEND_FUA = 1 << 3,     // Send FUA (Force Unit Access)
    NBD_FLAG_ROTATIONAL = 1 << 4,   // Use elevator algorithm - rotational media
    NBD_FLAG_SEND_TRIM = 1 << 5,    // Send TRIM (discard)
    NBD_FLAG_SEND_WRITE_ZEROES = 1 << 6,   // Send WRITE_ZEROES
    NBD_FLAG_SEND_DF = 1 << 7,             // Send DF (Do not Fragment)
};

////////////////////////////////////////////////////////////////////////////////
// Option requests

enum
{
    NBD_OPT_EXPORT_NAME = 1,
    NBD_OPT_ABORT = 2,
    NBD_OPT_LIST = 3,
    NBD_OPT_PEEK_EXPORT = 4,   // not in use
    NBD_OPT_STARTTLS = 5,
    NBD_OPT_INFO = 6,
    NBD_OPT_GO = 7,
    NBD_OPT_STRUCTURED_REPLY = 8,
    NBD_OPT_LIST_META_CONTEXT = 9,
    NBD_OPT_SET_META_CONTEXT = 10,

    // NBS options
    NBD_OPT_USE_NBS_ERRORS = 11,
};

////////////////////////////////////////////////////////////////////////////////
// Info types, used during NBD_REP_INFO

enum
{
    NBD_INFO_EXPORT = 0,
    NBD_INFO_NAME = 1,
    NBD_INFO_DESCRIPTION = 2,
    NBD_INFO_BLOCK_SIZE = 3,
};

////////////////////////////////////////////////////////////////////////////////
// Option reply types

#define NBD_REP_ERR(value) ((ui32(1) << 31) | (value))

enum
{
    NBD_REP_ACK = 1,            // Data sending finished.
    NBD_REP_SERVER = 2,         // Export description.
    NBD_REP_INFO = 3,           // NBD_OPT_INFO/GO.
    NBD_REP_META_CONTEXT = 4,   // NBD_OPT_{LIST,SET}_META_CONTEXT

    NBD_REP_ERR_UNSUP = NBD_REP_ERR(1),             // Unknown option
    NBD_REP_ERR_POLICY = NBD_REP_ERR(2),            // Server denied
    NBD_REP_ERR_INVALID = NBD_REP_ERR(3),           // Invalid length
    NBD_REP_ERR_PLATFORM = NBD_REP_ERR(4),          // Not compiled in
    NBD_REP_ERR_TLS_REQD = NBD_REP_ERR(5),          // TLS required
    NBD_REP_ERR_UNKNOWN = NBD_REP_ERR(6),           // Export unknown
    NBD_REP_ERR_SHUTDOWN = NBD_REP_ERR(7),          // Server shutting down
    NBD_REP_ERR_BLOCK_SIZE_REQD = NBD_REP_ERR(8),   // Need INFO_BLOCK_SIZE
};

inline bool OptionReplyTypeIsError(ui32 type)
{
    return (type & NBD_REP_ERR(0));
}

////////////////////////////////////////////////////////////////////////////////
// Request types

enum
{
    NBD_CMD_READ = 0,
    NBD_CMD_WRITE = 1,
    NBD_CMD_DISC = 2,
    NBD_CMD_FLUSH = 3,
    NBD_CMD_TRIM = 4,
    NBD_CMD_CACHE = 5,   // not in use
    NBD_CMD_WRITE_ZEROES = 6,
    NBD_CMD_BLOCK_STATUS = 7,
};

////////////////////////////////////////////////////////////////////////////////
// Request flags, sent from client to server during transmission phase

enum
{
    NBD_CMD_FLAG_FUA = 1 << 0,       // 'force unit access' during write
    NBD_CMD_FLAG_NO_HOLE = 1 << 1,   // don't punch hole on zero run
    NBD_CMD_FLAG_DF = 1 << 2,        // don't fragment structured read
    NBD_CMD_FLAG_REQ_ONE =
        1 << 3,   // only one extent in BLOCK_STATUS reply chunk
};

////////////////////////////////////////////////////////////////////////////////
// Structured reply flags

enum
{
    NBD_REPLY_FLAG_NONE = 0,
    NBD_REPLY_FLAG_DONE = 1 << 0,
};

////////////////////////////////////////////////////////////////////////////////
// Structured reply types

#define NBD_REPLY_ERR(value) ((1 << 15) | (value))

enum
{
    NBD_REPLY_TYPE_NONE = 0,
    NBD_REPLY_TYPE_OFFSET_DATA = 1,
    NBD_REPLY_TYPE_OFFSET_HOLE = 2,
    NBD_REPLY_TYPE_BLOCK_STATUS = 5,

    NBD_REPLY_TYPE_ERROR = NBD_REPLY_ERR(1),
    NBD_REPLY_TYPE_ERROR_OFFSET = NBD_REPLY_ERR(2),
};

inline bool ReplyTypeIsError(ui32 type)
{
    return (type & NBD_REPLY_ERR(0));
}

////////////////////////////////////////////////////////////////////////////////
// Error codes

enum
{
    NBD_SUCCESS = 0,
    NBD_EPERM = 1,
    NBD_EIO = 5,
    NBD_ENOMEM = 12,
    NBD_EINVAL = 22,
    NBD_ENOSPC = 28,
    NBD_EOVERFLOW = 75,
    NBD_ESHUTDOWN = 108,
};

////////////////////////////////////////////////////////////////////////////////

struct TServerHello
{
    ui64 Passwd;   // NBD_MAGIC
    ui64 Magic;    // NBD_OPTS_MAGIC
    ui16 Flags;    // NBD_FLAG
};

////////////////////////////////////////////////////////////////////////////////

struct TClientHello
{
    ui32 Flags;   // NBD_FLAG
};

////////////////////////////////////////////////////////////////////////////////

struct TOption
{
    ui64 Magic;    // NBD_OPTS_MAGIC
    ui32 Option;   // NBD_OPT
    ui32 Length;   // length of payload
};

////////////////////////////////////////////////////////////////////////////////

struct TExportInfoRequest
{
    TString Name;              // name of the export to query
    TVector<ui16> InfoTypes;   // NBD_INFO
};

////////////////////////////////////////////////////////////////////////////////

struct TExportInfo
{
    TString Name;
    ui64 Size;
    ui32 MinBlockSize;
    ui32 OptBlockSize;
    ui32 MaxBlockSize;
    ui16 Flags;
};

////////////////////////////////////////////////////////////////////////////////

struct TOptionReply
{
    ui64 Magic;    // NBD_REP_MAGIC
    ui32 Option;   // NBD_OPT
    ui32 Type;     // NBD_REP
    ui32 Length;   // length of payload
};

////////////////////////////////////////////////////////////////////////////////

struct TRequest
{
    ui32 Magic;    // NBD_REQUEST_MAGIC
    ui16 Flags;    // NBD_CMD_FLAG
    ui16 Type;     // NBD_CMD
    ui64 Handle;   // request handle
    ui64 From;     // start of requested range
    ui32 Length;   // length of requested range
};

////////////////////////////////////////////////////////////////////////////////

struct TSimpleReply
{
    ui32 Magic;    // NBD_SIMPLE_REPLY_MAGIC
    ui32 Error;    // NBD errror code
    ui64 Handle;   // request handle
};

////////////////////////////////////////////////////////////////////////////////

struct TStructuredReplyChunk
{
    ui32 Magic;    // NBD_STRUCTURED_REPLY_MAGIC
    ui16 Flags;    // NBD_REPLY_FLAG
    ui16 Type;     // NBD_REPLY_TYPE
    ui64 Handle;   // request handle
    ui32 Length;   // length of payload
};

struct TStructuredReadData
{
    ui64 Offset;
};

struct TStructuredReadHole
{
    ui64 Offset;
    ui32 DataLength;
};

struct TStructuredError
{
    ui32 Error;
    ui16 MessageLength;
};

struct TStructuredReply: TStructuredReplyChunk
{
    union {
        TStructuredReadData ReadData;
        TStructuredReadHole ReadHole;
        TStructuredError Error;
    };
};

////////////////////////////////////////////////////////////////////////////////

class TRequestWriter: public TBinaryWriter
{
public:
    TRequestWriter(IOutputStream& out)
        : TBinaryWriter(out)
    {}

    void WriteServerHello(ui16 flags);
    void WriteClientHello(ui32 flags);
    void WriteOption(ui32 option, TStringBuf optionData = {});
    void WriteExportInfoRequest(const TExportInfoRequest& request);
    void WriteExportList(const TExportInfo& exp);
    void WriteExportInfo(const TExportInfo& exp, ui16 type);
    void WriteOptionReply(ui32 option, ui32 type, TStringBuf replyData = {});
    void WriteRequest(const TRequest& request, TStringBuf requestData = {});
    void WriteRequest(const TRequest& request, const TSgList& requestData);
    void WriteSimpleReply(ui64 handle, ui32 error);
    void WriteStructuredDone(ui64 handle);
    void
    WriteStructuredReadData(ui64 handle, ui64 offset, ui32 length, bool final);
    void
    WriteStructuredReadHole(ui64 handle, ui64 offset, ui32 length, bool final);
    void WriteStructuredError(ui64 handle, ui32 error, TStringBuf message);
};

////////////////////////////////////////////////////////////////////////////////

class TRequestReader: public TBinaryReader
{
public:
    TRequestReader(IInputStream& in)
        : TBinaryReader(in)
    {}

    bool ReadServerHello(TServerHello& hello);
    bool ReadClientHello(TClientHello& hello);
    bool ReadOption(TOption& option, TBuffer& optionData);
    bool ReadExportInfoRequest(TExportInfoRequest& request);
    bool ReadExportList(TExportInfo& exp);
    bool ReadExportInfo(TExportInfo& exp, ui16& type);
    bool ReadOptionReply(TOptionReply& reply, TBuffer& replyData);
    bool ReadRequest(TRequest& request);
    bool ReadSimpleReply(TSimpleReply& reply);
    bool ReadStructuredReply(TStructuredReply& reply);
    size_t ReadStructuredReplyData(TStructuredReply& reply, TBuffer& data);
    size_t ReadStructuredReplyData(
        TStructuredReply& reply,
        const TSgList& data);
};

////////////////////////////////////////////////////////////////////////////////

class TBufferRequestWriter: public TRequestWriter
{
private:
    TBufferOutput Out;

public:
    TBufferRequestWriter()
        : TRequestWriter(Out)
    {}

    TBuffer& Buffer()
    {
        return Out.Buffer();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TBufferRequestReader: public TRequestReader
{
private:
    TBufferInput In;

public:
    TBufferRequestReader(TBuffer& buffer)
        : TRequestReader(In)
        , In(buffer)
    {}
};

}   // namespace NCloud::NBlockStore::NBD
