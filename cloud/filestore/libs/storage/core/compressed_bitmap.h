#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/compressed_bitmap.h>
#include <cloud/storage/core/libs/common/error.h>

#include <memory>

namespace NCloud::NFileStore::NProtoPrivate {
class TCompressedBitmapData;
}   // namespace NCloud::NFileStore::NProtoPrivate

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TResultOrError<std::unique_ptr<NCloud::TCompressedBitmap>> LoadCompressedBitmap(
    const NProtoPrivate::TCompressedBitmapData& proto,
    ui64 minBitCount,
    ui64 maxBitCount);

void SaveCompressedBitmap(
    const NCloud::TCompressedBitmap& bitmap,
    ui64 bitCount,
    NProtoPrivate::TCompressedBitmapData& proto);

NProto::TError ValidateCompressedBitmapData(
    const NProtoPrivate::TCompressedBitmapData& bitmap,
    ui64 maxBitCount);

}   // namespace NCloud::NFileStore::NStorage
