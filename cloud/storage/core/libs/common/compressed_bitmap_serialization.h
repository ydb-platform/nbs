#pragma once

#include "public.h"

#include "compressed_bitmap.h"
#include "error.h"

#include <memory>

namespace NCloud::NProto {
class TCompressedBitmapData;
}   // namespace NCloud::NProto

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

TResultOrError<std::unique_ptr<TCompressedBitmap>> LoadCompressedBitmap(
    const NProto::TCompressedBitmapData& proto,
    ui64 minBitCount,
    ui64 maxBitCount);

void SaveCompressedBitmap(
    const TCompressedBitmap& bitmap,
    ui64 bitCount,
    NProto::TCompressedBitmapData& proto);

NProto::TError ValidateCompressedBitmapData(
    const NProto::TCompressedBitmapData& bitmap,
    ui64 maxBitCount);

}   // namespace NCloud
