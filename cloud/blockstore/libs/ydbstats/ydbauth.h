#pragma once

#include "public.h"

#include <cloud/storage/core/libs/iam/iface/public.h>

#include <ydb/public/sdk/cpp/client/ydb_types/credentials/credentials.h>

namespace NCloud::NBlockStore::NYdbStats {

////////////////////////////////////////////////////////////////////////////////

NYdb::TCredentialsProviderFactoryPtr CreateIamCredentialsProviderFactory(
    NIamClient::IIamTokenClientPtr client);

}   // namespace NCloud::NBlockStore::NYdbStats
