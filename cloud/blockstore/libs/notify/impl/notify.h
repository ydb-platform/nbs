#pragma once

#include <cloud/blockstore/libs/notify/iface/notify.h>

#include <cloud/storage/core/libs/iam/iface/public.h>

namespace NCloud::NBlockStore::NNotify {

////////////////////////////////////////////////////////////////////////////////

struct IJsonGenerator
{
    virtual ~IJsonGenerator() = default;

    virtual NJson::TJsonMap Generate(const TNotification& data) = 0;
};

using IJsonGeneratorPtr = std::unique_ptr<IJsonGenerator>;

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateService(
    TNotifyConfigPtr config,
    NCloud::NIamClient::IIamTokenClientPtr iamTokenClientPtr,
    IJsonGeneratorPtr jsonGenerator);

IServicePtr CreateService(
    TNotifyConfigPtr config,
    NCloud::NIamClient::IIamTokenClientPtr iamTokenClientPtr);

}   // namespace NCloud::NBlockStore::NNotify
