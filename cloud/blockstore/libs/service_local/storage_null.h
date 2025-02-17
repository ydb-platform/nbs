#pragma once

#include "public.h"

#include <cloud/blockstore/libs/service/public.h>

#include <library/cpp/threading/future/core/future.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

IStorageProviderPtr CreateNullStorageProvider();

IStoragePtr CreateNullStorage();

IStoragePtr CreateControlledStorage(NThreading::TFuture<void> waitFor);

}   // namespace NCloud::NBlockStore::NServer
