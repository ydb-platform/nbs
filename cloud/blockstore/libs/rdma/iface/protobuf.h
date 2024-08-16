#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/block_data_ref.h>
#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/hash.h>
#include <util/generic/strbuf.h>
#include <util/network/iovec.h>

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

//  ==========================================
//  |            Serialized message          |
//  ------------------------------------------
//  |  TProtoHeader {           |            |
//  |     ui32 MsgId : 16;      |  2 byte    |
//  |     ui32 ProtoLen : 16;   |  2 byte    |
//  |     ui32 DataLen;         |  4 byte    |
//  |  }                        |            |
//  ------------------------------------------
//  |  Proto (SerializeToArray) |  protoLen  |
//  ------------------------------------------
//  |  Data                     |  dataLen   |
//  ------------------------------------------

// Thread-safe. Public methods can be called from any thread.
class TProtoMessageSerializer
{
    struct IProtoFactory
    {
        virtual ~IProtoFactory() = default;

        virtual TProtoMessagePtr Create() const = 0;
    };

    using IProtoFactoryPtr = std::unique_ptr<IProtoFactory>;

    template <typename T>
    struct TProtoFactory : IProtoFactory
    {
        TProtoMessagePtr Create() const override
        {
            return std::make_unique<T>();
        }
    };

private:
    THashMap<ui32, IProtoFactoryPtr> Messages;

public:
    static size_t MessageByteSize(const TProtoMessage& proto, size_t dataLen);

    static size_t Serialize(
        TStringBuf buffer,
        ui32 msgId,
        ui32 flags,
        const TProtoMessage& proto,
        TBlockDataRefSpan data = {});

    static size_t Serialize(
        TStringBuf buffer,
        ui32 msgId,
        ui32 flags,
        const TProtoMessage& proto,
        size_t dataLen);

    struct TParseResult
    {
        ui32 MsgId;
        ui32 Flags;
        TProtoMessagePtr Proto;
        TStringBuf Data;
    };

    [[nodiscard]] TResultOrError<TParseResult> Parse(TStringBuf buffer) const;

protected:
    template <typename T>
    void RegisterProto(ui32 msgId)
    {
        RegisterProto(msgId, std::make_unique<TProtoFactory<T>>());
    }

private:
    void RegisterProto(ui32 msgId, IProtoFactoryPtr factory);
    TProtoMessagePtr CreateProto(ui32 msgId) const;
};

////////////////////////////////////////////////////////////////////////////////

size_t SerializeError(ui32 code, TStringBuf message, TStringBuf buffer);

NProto::TError ParseError(TStringBuf buffer);

}   // namespace NCloud::NBlockStore::NRdma
