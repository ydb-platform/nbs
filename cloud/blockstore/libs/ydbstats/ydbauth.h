#pragma once

#include "public.h"

#include <cloud/storage/core/libs/iam/iface/public.h>

#include <contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>

namespace NCloud::NBlockStore::NYdbStats {

////////////////////////////////////////////////////////////////////////////////

NYdb::TCredentialsProviderFactoryPtr CreateIamCredentialsProviderFactory(
    NIamClient::IIamTokenClientPtr client);

}   // namespace NCloud::NBlockStore::NYdbStats
