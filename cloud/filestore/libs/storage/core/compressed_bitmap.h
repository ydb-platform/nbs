#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/compressed_bitmap.h>
#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NFileStore::NProtoPrivate {
class TCompressedBitmapData;
}   // namespace NCloud::NFileStore::NProtoPrivate

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NCloud::TCompressedBitmap LoadCompressedBitmap(
    const NProtoPrivate::TCompressedBitmapData& proto,
    ui64 minBitCount);

void SaveCompressedBitmap(
    const NCloud::TCompressedBitmap& bitmap,
    ui64 bitCount,
    NProtoPrivate::TCompressedBitmapData& proto);

NProto::TError ValidateCompressedBitmapData(
    const NProtoPrivate::TCompressedBitmapData& bitmap,
    ui64 maxBitCount);

}   // namespace NCloud::NFileStore::NStorage
