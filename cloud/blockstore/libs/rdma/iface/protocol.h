#pragma once

#include "public.h"

#include <util/generic/ylimits.h>
#include <util/system/defaults.h>

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

// usage of fixed-size structures greatly simplifies message passing
constexpr size_t RDMA_PRIVATE_SIZE = 16;
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
    RDMA_PROTO_VERSION_0        = 0,
    RDMA_PROTO_VERSION_1        = 1,
    RDMA_PROTO_VERSION          = RDMA_PROTO_VERSION_1,
};

enum {
    RDMA_PROTO_OK               = 0,
    RDMA_PROTO_INVALID_REQUEST  = 1,
    RDMA_PROTO_CONFIG_MISMATCH  = 2,
    RDMA_PROTO_THROTTLED        = 3,
    RDMA_PROTO_FAIL             = 4,
    RDMA_PROTO_CANCELED         = 5,
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

inline void InitMessageHeader(void* msg, int version)
{
    auto* hdr = static_cast<TMessageHeader*>(msg);
    hdr->Magic[0] = 'R';
    hdr->Magic[1] = 'D';
    hdr->Magic[2] = 'M';
    hdr->Magic[3] = '0' + version;
}

inline int ParseMessageHeader(const void* msg)
{
    const auto* hdr = static_cast<const TMessageHeader*>(msg);
    if (hdr->Magic[0] == 'R' &&
        hdr->Magic[1] == 'D' &&
        hdr->Magic[2] == 'M' &&
        hdr->Magic[3]  > '0')
    {
        return hdr->Magic[3] - '0';
    }

    return RDMA_PROTO_VERSION_0;
}

////////////////////////////////////////////////////////////////////////////////

struct Y_PACKED TConnectMessage
{
    union {
        struct {
            TMessageHeader Header;
            ui32 QueueSize : 16;
            ui32 Unused : 16;
            ui32 MaxBufferSize;
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
        };
        ui8 Padding[RDMA_PRIVATE_SIZE];
    };
};

static_assert(sizeof(TAcceptMessage) == RDMA_PRIVATE_SIZE);

////////////////////////////////////////////////////////////////////////////////

struct Y_PACKED TRejectMessage
{
    union {
        struct {
            TMessageHeader Header;
            ui32 Status : 16;
            ui32 QueueSize : 16;
            ui32 MaxBufferSize;
        };
        ui8 Padding[RDMA_PRIVATE_SIZE];
    };
};

static_assert(sizeof(TRejectMessage) == RDMA_PRIVATE_SIZE);

////////////////////////////////////////////////////////////////////////////////

struct Y_PACKED TBufferDesc
{
    ui64 Address = 0;
    ui32 Length = 0;
    ui32 Key = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct Y_PACKED TRequestMessage
{
    union {
        struct {
            TMessageHeader Header;
            ui32 ReqId : 16;
            ui32 Unused : 16;
            TBufferDesc In;
            TBufferDesc Out;
        };
        ui8 Padding[RDMA_REQUEST_SIZE];
    };
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

}   // namespace NCloud::NBlockStore::NRdma
