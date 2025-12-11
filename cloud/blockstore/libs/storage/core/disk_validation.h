#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/core/config.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/media.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NProto::TError ValidateBlockSize(
    ui64 blockSize,
    NProto::EStorageMediaKind mediaKind);

}   // namespace NCloud::NBlockStore::NStorage
