#include "zero_block_stats.h"

#include <util/generic/strbuf.h>

#include <cstring>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

bool IsAllZeroes(TStringBuf data)
{
    return data.empty() ||
           data[0] == 0 &&
               memcmp(data.data(), data.data() + 1, data.size() - 1) == 0;
}

////////////////////////////////////////////////////////////////////////////////

class TZeroBlockScanner
{
private:
    const TByteRange Range;

    ui64 Offset;
    bool BlockIsZero = true;
    TZeroBlockStats Stats;

public:
    explicit TZeroBlockScanner(TByteRange range)
        : Range(range)
        , Offset(range.Offset)
    {}

    void Add(TStringBuf data)
    {
        data = data.Head(Range.End() - Offset);

        while (data) {
            const ui64 blockStart = Offset - Offset % Range.BlockSize;
            const ui64 blockEnd = blockStart + Range.BlockSize;
            const bool wholeBlock =
                blockStart >= Range.Offset && blockEnd <= Range.End();

            const auto part = data.Head(blockEnd - Offset);
            const bool shouldCheck =
                Stats.AllZeroes || wholeBlock && BlockIsZero;
            if (shouldCheck && !IsAllZeroes(part)) {
                Stats.AllZeroes = false;
                BlockIsZero = false;
            }

            Offset += part.size();
            data.Skip(part.size());

            if (Offset == blockEnd) {
                if (wholeBlock && BlockIsZero) {
                    Stats.ZeroBlockBytes += Range.BlockSize;
                }
                BlockIsZero = true;
            }
        }
    }

    TZeroBlockStats Finish() const
    {
        return Stats;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TZeroBlockStats CalculateZeroBlockStats(
    const NProto::TWriteDataRequest& request,
    TByteRange range)
{
    TZeroBlockScanner scanner(range);

    if (request.GetIovecs().empty()) {
        TStringBuf buffer(request.GetBuffer());
        buffer.Skip(request.GetBufferOffset());
        scanner.Add(buffer);
    } else {
        for (const auto& iovec: request.GetIovecs()) {
            scanner.Add(TStringBuf(
                reinterpret_cast<const char*>(iovec.GetBase()),
                iovec.GetLength()));
        }
    }

    return scanner.Finish();
}

}   // namespace NCloud::NFileStore::NStorage
