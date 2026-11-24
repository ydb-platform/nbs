#pragma once

#include "public.h"

#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/protos/media.pb.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

TResultOrError<NCloud::NProto::EStorageMediaKind> ParseFileStoreMediaKind(
    TStringBuf s);

TString FastShardStorageKindToString(NProto::EFastShardStorageKind kind);

TResultOrError<NProto::EFastShardStorageKind> ParseFastShardStorageKind(
    TStringBuf s);

}   // namespace NCloud::NFileStore
