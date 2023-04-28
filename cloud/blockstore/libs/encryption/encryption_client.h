#pragma once

#include "public.h"

#include <cloud/blockstore/public/api/protos/encryption.pb.h>

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateEncryptionClient(
    IBlockStorePtr client,
    ILoggingServicePtr logging,
    IEncryptorPtr encryptor,
    TString encryptionKeyHash);

IBlockStorePtr CreateSnapshotEncryptionClient(
    IBlockStorePtr client,
    ILoggingServicePtr logging,
    TString encryptionKeyHash);

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IBlockStorePtr> TryToCreateEncryptionClient(
    IBlockStorePtr client,
    ILoggingServicePtr logging,
    const NProto::TEncryptionSpec& encryptionSpec);

}   // namespace NCloud::NBlockStore::NClient
