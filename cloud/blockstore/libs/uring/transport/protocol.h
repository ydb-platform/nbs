#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/protos/error.pb.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

#include <optional>
#include <type_traits>

namespace NCloud::NBlockStore::NUring {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 MAGIC = 0x5553424E;   // "NBSU" little-endian
constexpr ui16 PROTOCOL_VERSION = 1;
constexpr ui32 MAX_IOV = 64;

enum ERole : ui8
{
    ROLE_MAIN = 1,
    ROLE_DATA = 2,
};

enum EOobWire : ui8
{
    OOB_WIRE_OFF = 0,
    OOB_WIRE_AUTO = 1,
    OOB_WIRE_REQUIRED = 2,
};

enum EStatus : ui8
{
    STATUS_OK = 0,
    STATUS_VERSION_MISMATCH = 1,
    STATUS_CONFIG_MISMATCH = 2,
    STATUS_BAD_TOKEN = 3,
};

enum EFrameType : ui8
{
    FRAME_REQUEST = 1,
    FRAME_RESPONSE = 2,
    FRAME_ERROR = 3,
};

enum EFrameFlags : ui8
{
    FLAG_INLINE = 1,
    FLAG_OOB = 2,
};

////////////////////////////////////////////////////////////////////////////////

struct THello
{
    ui32 Magic = MAGIC;
    ui16 Version = PROTOCOL_VERSION;
    ui8 Role = ROLE_MAIN;
    ui8 OobMode = OOB_WIRE_AUTO;
    ui32 MaxProtoSize = 0;
    ui32 MaxDataSize = 0;
    ui32 MaxInflightBytes = 0;
    ui32 OobThreshold = 0;
};

struct TWelcome
{
    ui32 Magic = MAGIC;
    ui16 Version = PROTOCOL_VERSION;
    ui8 Status = STATUS_OK;
    ui8 OobMode = OOB_WIRE_OFF;   // negotiated: OFF or REQUIRED(on)
    ui64 DataToken = 0;
    ui32 MaxProtoSize = 0;
    ui32 MaxDataSize = 0;
    ui32 MaxInflightBytes = 0;
    ui32 OobThreshold = 0;
};

struct TDataHello
{
    ui32 Magic = MAGIC;
    ui16 Version = PROTOCOL_VERSION;
    ui8 Role = ROLE_DATA;
    ui8 Pad = 0;
    ui64 DataToken = 0;
};

struct TDataWelcome
{
    ui32 Magic = MAGIC;
    ui16 Version = PROTOCOL_VERSION;
    ui8 Status = STATUS_OK;
    ui8 Pad = 0;
};

struct TFrameHeader
{
    ui32 Magic = MAGIC;
    ui8 Type = 0;
    ui8 Flags = 0;
    ui16 Pad = 0;
    ui64 RequestId = 0;
    ui32 ProtoLen = 0;
    ui32 DataLen = 0;
};

struct TDataHeader
{
    ui32 Magic = MAGIC;
    ui32 DataLen = 0;
    ui64 RequestId = 0;
};

static_assert(sizeof(THello) == 24);
static_assert(sizeof(TWelcome) == 32);
static_assert(sizeof(TDataHello) == 16);
static_assert(sizeof(TDataWelcome) == 8);
static_assert(sizeof(TFrameHeader) == 24);
static_assert(sizeof(TDataHeader) == 16);
static_assert(std::is_trivially_copyable_v<TFrameHeader>);

////////////////////////////////////////////////////////////////////////////////

inline ui8 ToWire(EOobMode mode)
{
    switch (mode) {
        case EOobMode::Off:
            return OOB_WIRE_OFF;
        case EOobMode::Required:
            return OOB_WIRE_REQUIRED;
        default:
            return OOB_WIRE_AUTO;
    }
}

inline EOobMode FromWire(ui8 mode)
{
    switch (mode) {
        case OOB_WIRE_OFF:
            return EOobMode::Off;
        case OOB_WIRE_REQUIRED:
            return EOobMode::Required;
        default:
            return EOobMode::Auto;
    }
}

// nullopt — incompatible; false — OOB off; true — OOB on
inline std::optional<bool> NegotiateOob(EOobMode local, EOobMode remote)
{
    const bool localOff = local == EOobMode::Off;
    const bool remoteOff = remote == EOobMode::Off;
    const bool localReq = local == EOobMode::Required;
    const bool remoteReq = remote == EOobMode::Required;
    if ((localOff && remoteReq) || (localReq && remoteOff)) {
        return std::nullopt;
    }
    return !(localOff || remoteOff);
}

inline TString SerializeError(ui32 code, TStringBuf message)
{
    NProto::TError error;
    error.SetCode(code);
    error.SetMessage(TString(message));
    return error.SerializeAsString();
}

inline NProto::TError ParseError(TStringBuf buffer)
{
    NProto::TError error;
    if (buffer.empty() || !error.ParseFromArray(buffer.data(), buffer.size())) {
        return MakeError(E_FAIL, "unparsable error payload");
    }
    if (!HasError(error)) {
        return MakeError(E_FAIL, "error payload carries a success code");
    }
    return error;
}

}   // namespace NCloud::NBlockStore::NUring
