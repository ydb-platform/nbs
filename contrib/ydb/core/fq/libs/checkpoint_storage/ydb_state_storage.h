#pragma once

#include "state_storage.h"

#include <contrib/ydb/library/security/ydb_credentials_provider_factory.h>
#include <contrib/ydb/core/fq/libs/config/protos/storage.pb.h>
#include <contrib/ydb/core/fq/libs/shared_resources/shared_resources.h>

namespace NFq {

////////////////////////////////////////////////////////////////////////////////

TStateStoragePtr NewYdbStateStorage(
    const NConfig::TYdbStorageConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources);

} // namespace NFq
