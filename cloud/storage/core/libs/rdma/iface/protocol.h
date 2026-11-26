#pragma once

#include "public.h"

#include <util/generic/strbuf.h>
#include <util/generic/ylimits.h>
#include <util/system/defaults.h>
#include <util/system/yassert.h>

#include <cstring>
#include <type_traits>

namespace NCloud::NStorage::NRdma {

////////////////////////////////////////////////////////////////////////////////

// usage of fixed-size structures greatly simplifies message passing
constexpr size_t RDMA_PRIVATE_SIZE = 16;
// private data of the connect request of an RC connection
constexpr size_t RDMA_MAX_CONNECT_PRIVATE_SIZE = 56;
constexpr size_t RDMA_MAX_EXTERNAL_DATA_SIZE = 24;
constexpr size_t RDMA_REQUEST_SIZE = 64;
constexpr size_t RDMA_RESPONSE_SIZE = 16;
constexpr size_t RDMA_PROTO_HEADER_SIZE = 8;

// request identifier should fit into 16 bits
constexpr size_t RDMA_MAX_REQID = Max<ui16>();

// proto len should fit into 16 bits
constexpr size_t RDMA_MAX_PROTO_LEN = Max<ui16>();
constexpr size_t RDMA_MAX_DATA_LEN = Max<ui32>();

////////////////////////////////////////////////////////////////////////////////

enum {
    RDMA_PROTO_OK               = 0,
    RDMA_PROTO_INVALID_REQUEST  = 1,
    RDMA_PROTO_CONFIG_MISMATCH  = 2,
    RDMA_PROTO_THROTTLED        = 3,
    RDMA_PROTO_FAIL             = 4,
    RDMA_PROTO_REJECTED         = 5,
};

enum {
    RDMA_PROTO_FLAG_NONE = 0,
    // encode data at the end of the buffer like this:
    //
    //              buffer
    // |--------------+-------+--------+------|
    // | TProtoHeader | Proto | unused | Data |
    // |--------------+-------+--------+------|
    //
    // instead of:
    //              buffer
    // |--------------+-------+------+--------|
    // | TProtoHeader | Proto | Data | unused |
    // |--------------+-------+------+--------|
    //
    //
    // If buffer is allocated in chunks of 4k
    // and data is allocated in multiple of block size (512, 4k)
    // then data address is also aligned to (512, 4k)
    RDMA_PROTO_FLAG_DATA_AT_THE_END = 1,
};

////////////////////////////////////////////////////////////////////////////////

struct Y_PACKED TMessageHeader
{
    ui8 Magic[4];
};

inline void InitMessageHeader(void* msg)
{
    auto* hdr = static_cast<TMessageHeader*>(msg);
    hdr->Magic[0] = 'R';
    hdr->Magic[1] = 'D';
    hdr->Magic[2] = 'M';
    hdr->Magic[3] = '3';
}

inline bool IsValidMessageHeader(const void* msg)
{
    const auto* hdr = static_cast<const TMessageHeader*>(msg);
    return hdr->Magic[0] == 'R' &&
           hdr->Magic[1] == 'D' &&
           hdr->Magic[2] == 'M' &&
           hdr->Magic[3] == '3';
}

////////////////////////////////////////////////////////////////////////////////

struct Y_PACKED TConnectMessage
{
    union {
        struct {
            TMessageHeader Header;
            ui32 SendQueueSize : 16;
            ui32 RecvQueueSize : 16;
            ui32 MaxBufferSize;
            // largest request payload sent eagerly, 0 disables
            ui32 MaxEagerRequestBytes;
        };
        ui8 Padding[RDMA_PRIVATE_SIZE];
    };
};

static_assert(sizeof(TConnectMessage) == RDMA_PRIVATE_SIZE);

////////////////////////////////////////////////////////////////////////////////

struct Y_PACKED TAcceptMessage
{
    union {
        struct {
            TMessageHeader Header;
            ui32 Unused : 16;
            ui32 KeepAliveTimeout : 16;
            // effective eager limit, min(client request, server config)
            ui32 MaxEagerRequestBytes;
        };
        ui8 Padding[RDMA_PRIVATE_SIZE];
    };
};

static_assert(sizeof(TAcceptMessage) == RDMA_PRIVATE_SIZE);

////////////////////////////////////////////////////////////////////////////////

struct Y_PACKED TRejectMessage
{
    union {
        struct
        {
            TMessageHeader Header;
            ui32 Status : 16;
            ui32 Unused : 16;   // padding
            ui32 SendQueueSize : 16;
            ui32 RecvQueueSize : 16;
            ui32 MaxBufferSize;
        };

        ui8 Padding[RDMA_PRIVATE_SIZE];
    };
};

static_assert(sizeof(TRejectMessage) == RDMA_PRIVATE_SIZE);

////////////////////////////////////////////////////////////////////////////////

// Options follow the fixed part of a connection message:
//
// |---------------+--------+--------+-----|
// | fixed message | option | option | ... |
// |---------------+--------+--------+-----|
//
// An option is a flag (ui8) followed either by a value of a fixed size or by a
// size (ui8) and that many bytes of data, the flag tells which:
//
// |------+-------|    |------+------+------|
// | flag | value | or | flag | size | data |
// |------+-------|    |------+------+------|
//
// Options are written in the ascending order of flags. A receiver reads the
// options it knows and ignores everything starting from the first flag it does
// not know, so a new option gets a flag greater than the existing ones. Flags
// are not zero: private data comes padded with zeros.

enum {
    // size + data, the data is handed to IServerHandler::HandleConnect
    RDMA_CONNECT_OPTION_EXTERNAL_DATA = 1,
};

static_assert(
    sizeof(TConnectMessage) + 2 * sizeof(ui8) + RDMA_MAX_EXTERNAL_DATA_SIZE <=
    RDMA_MAX_CONNECT_PRIVATE_SIZE);

class TOptionWriter
{
private:
    char* const Begin;
    char* const End;
    char* Ptr;
    ui8 LastFlag = 0;

public:
    TOptionWriter(void* buffer, size_t capacity)
        : Begin(static_cast<char*>(buffer))
        , End(Begin + capacity)
        , Ptr(Begin)
    {}

    size_t Size() const
    {
        return Ptr - Begin;
    }

    template <typename T>
    void WriteValue(ui8 flag, const T& value)
    {
        static_assert(std::is_trivially_copyable_v<T>);

        WriteFlag(flag);
        Write(&value, sizeof(value));
    }

    void WriteData(ui8 flag, TStringBuf data)
    {
        Y_ABORT_UNLESS(data.size() <= Max<ui8>());
        const ui8 size = static_cast<ui8>(data.size());

        WriteFlag(flag);
        Write(&size, sizeof(size));
        Write(data.data(), data.size());
    }

private:
    void WriteFlag(ui8 flag)
    {
        Y_ABORT_UNLESS(flag > LastFlag);
        LastFlag = flag;
        Write(&flag, sizeof(flag));
    }

    void Write(const void* data, size_t size)
    {
        Y_ABORT_UNLESS(size <= static_cast<size_t>(End - Ptr));
        if (size) {
            memcpy(Ptr, data, size);
            Ptr += size;
        }
    }
};

// An option that is not the next one is absent: the reader leaves the value
// as it is and returns true. False means that the option is cut short.
class TOptionReader
{
private:
    const char* Ptr;
    const char* const End;
    ui8 LastFlag = 0;

public:
    TOptionReader(const void* options, size_t size)
        : Ptr(static_cast<const char*>(options))
        , End(Ptr + size)
    {}

    template <typename T>
    bool ReadValue(ui8 flag, T* value)
    {
        static_assert(std::is_trivially_copyable_v<T>);

        if (!IsNext(flag)) {
            return true;
        }
        if (Left() < sizeof(flag) + sizeof(T)) {
            return false;
        }

        memcpy(value, Ptr + sizeof(flag), sizeof(T));
        Ptr += sizeof(flag) + sizeof(T);
        return true;
    }

    bool ReadData(ui8 flag, TStringBuf* data)
    {
        if (!IsNext(flag)) {
            return true;
        }
        if (Left() < sizeof(flag) + sizeof(ui8)) {
            return false;
        }

        const size_t size = static_cast<ui8>(Ptr[sizeof(flag)]);
        if (Left() < sizeof(flag) + sizeof(ui8) + size) {
            return false;
        }

        *data = TStringBuf(Ptr + sizeof(flag) + sizeof(ui8), size);
        Ptr += sizeof(flag) + sizeof(ui8) + size;
        return true;
    }

private:
    bool IsNext(ui8 flag)
    {
        Y_ABORT_UNLESS(flag > LastFlag);
        LastFlag = flag;
        return Ptr < End && static_cast<ui8>(*Ptr) == flag;
    }

    size_t Left() const
    {
        return End - Ptr;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct Y_PACKED TBufferDesc
{
    ui64 Address = 0;
    ui32 Length = 0;
    ui32 RKey = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct Y_PACKED TRequestMessage
{
    TMessageHeader Header;
    ui32 ReqId : 16;
    ui32 Unused : 16;
    TBufferDesc In;
    TBufferDesc Out;
    ui8 Padding
        [RDMA_REQUEST_SIZE - sizeof(Header) - sizeof(ui32) -
         sizeof(TBufferDesc) - sizeof(TBufferDesc)];
};

static_assert(sizeof(TRequestMessage) == RDMA_REQUEST_SIZE);

////////////////////////////////////////////////////////////////////////////////

struct Y_PACKED TResponseMessage
{
    union {
        struct {
            TMessageHeader Header;
            ui32 ReqId : 16;
            ui32 Status : 16;
            ui32 ResponseBytes;
        };
        ui8 Padding[RDMA_RESPONSE_SIZE];
    };
};

static_assert(sizeof(TResponseMessage) == RDMA_RESPONSE_SIZE);

////////////////////////////////////////////////////////////////////////////////

struct Y_PACKED TProtoHeader
{
    ui32 MsgId : 8;
    ui32 Flags : 8;
    ui32 ProtoLen : 16;
    ui32 DataLen;
};

static_assert(sizeof(TProtoHeader) == RDMA_PROTO_HEADER_SIZE);

}   // namespace NCloud::NStorage::NRdma
