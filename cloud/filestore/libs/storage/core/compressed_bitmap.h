#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/compressed_bitmap.h>

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

}   // namespace NCloud::NFileStore::NStorage
