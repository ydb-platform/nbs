#pragma once

#include "public.h"

#include <cloud/blockstore/public/api/protos/encryption.pb.h>
#include <cloud/blockstore/public/api/protos/volume.pb.h>

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateEncryptionClient(
    IBlockStorePtr client,
    ILoggingServicePtr logging,
    IEncryptorPtr encryptor,
    NProto::TEncryptionDesc encryptionDesc);

IBlockStorePtr CreateSnapshotEncryptionClient(
    IBlockStorePtr client,
    ILoggingServicePtr logging,
    NProto::TEncryptionDesc encryptionDesc);

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IBlockStorePtr> TryToCreateEncryptionClient(
    IBlockStorePtr client,
    ILoggingServicePtr logging,
    IEncryptionKeyProviderPtr encryptionKeyProvider,
    const NProto::TEncryptionSpec& encryptionSpec);

}   // namespace NCloud::NBlockStore::NClient
