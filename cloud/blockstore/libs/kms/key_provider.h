#pragma once

#include "public.h"

#include <cloud/blockstore/libs/encryption/public.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/coroutine/public.h>
#include <cloud/storage/core/libs/iam/iface/public.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

IKmsKeyProviderPtr CreateKmsKeyProvider(
    TExecutorPtr executor,
    NIamClient::IIamTokenClientPtr iamTokenClient,
    IComputeClientPtr computeClient,
    IKmsClientPtr kmsClient);

}   // namespace NCloud::NBlockStore
