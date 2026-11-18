#include "zero_block_stats.h"

#include <cloud/storage/core/libs/common/zeroes.h>

#include <util/generic/strbuf.h>
#include <util/system/align.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TZeroBlockStats CalculateZeroBlockStats(
    const NProto::TWriteDataRequest& request,
    ui32 blockSize)
{
    TZeroBlockStats stats;

    ui64 offset = request.GetOffset();
    bool blockIsZero = true;

    auto add = [&](TStringBuf data)
    {
        stats.Bytes += data.size();

        while (data) {
            const ui64 blockStart = AlignDown<ui64>(offset, blockSize);
            const ui64 blockEnd = blockStart + blockSize;
            const auto part = data.Head(blockEnd - offset);
            if (!IsAllZeroes(part.data(), part.size())) {
                stats.AllZeroes = false;
                blockIsZero = false;
            }

            offset += part.size();
            data.Skip(part.size());

            if (offset == blockEnd) {
                if (blockIsZero && blockStart >= request.GetOffset()) {
                    stats.ZeroBlockBytes += blockSize;
                }
                blockIsZero = true;
            }
        }
    };

    if (request.GetIovecs().empty()) {
        add(request.GetBuffer());
    } else {
        for (const auto& iovec: request.GetIovecs()) {
            add(TStringBuf(
                reinterpret_cast<const char*>(iovec.GetBase()),
                iovec.GetLength()));
        }
    }

    return stats;
}

}   // namespace NCloud::NFileStore::NStorage
