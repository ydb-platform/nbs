#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/hash.h>
#include <util/generic/strbuf.h>
#include <util/network/iovec.h>

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

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

    size_t Serialize(
        TStringBuf buffer,
        ui32 msgId,
        const TProtoMessage& proto,
        TContIOVector data) const;

    struct TParseResult
    {
        ui32 MsgId;
        TProtoMessagePtr Proto;
        TStringBuf Data;
    };

    TResultOrError<TParseResult> Parse(TStringBuf buffer) const;

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

}   // namespace NCloud::NBlockStore::NRdma
