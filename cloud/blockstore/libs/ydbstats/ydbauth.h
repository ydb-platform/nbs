#pragma once

#include "public.h"

#include <cloud/storage/core/libs/iam/iface/client.h>
#include <cloud/storage/core/libs/iam/iface/public.h>

#include <contrib/ydb/public/sdk/cpp/client/ydb_types/credentials/credentials.h>

namespace NCloud::NBlockStore::NYdbStats {

////////////////////////////////////////////////////////////////////////////////

NYdb::TCredentialsProviderFactoryPtr CreateIamCredentialsProviderFactory(
    NIamClient::TTokenInfo initialToken,
    NIamClient::IIamTokenClientPtr client);

}   // namespace NCloud::NBlockStore::NYdbStats
