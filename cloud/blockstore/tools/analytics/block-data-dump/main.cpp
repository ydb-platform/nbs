#include "options.h"

#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/public.h>

#include <library/cpp/digest/crc32c/crc32c.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/system/file.h>

#include <cinttypes>
#include <cstdio>
#include <memory>

using namespace NCloud::NBlockStore;

namespace {

////////////////////////////////////////////////////////////////////////////////

TString FormatTimestamp(ui64 mcs)
{
    return TInstant::MicroSeconds(mcs).ToStringLocalUpToSeconds();
}

void Dump(const TBlockData& data)
{
    std::printf(
        "RequestNumber:    %20" PRIu64 " [0x%016" PRIx64
        "]\n"
        "PartNumber:       %20" PRIu64 " [0x%016" PRIx64
        "]\n"
        "BlockIndex:       %20" PRIu64 " [0x%016" PRIx64
        "]\n"
        "RangeIdx:         %20" PRIu64 " [0x%016" PRIx64
        "]\n"
        "RequestTimestamp: %s [0x%016" PRIx64
        "]\n"
        "TestTimestamp:    %s [0x%016" PRIx64
        "]\n"
        "TestId:           %20" PRIu64 " [0x%016" PRIx64
        "]\n"
        "Checksum:         %20" PRIu64 " [0x%016" PRIx64 "]\n",
        data.RequestNumber,
        data.RequestNumber,
        data.PartNumber,
        data.PartNumber,
        data.BlockIndex,
        data.BlockIndex,
        data.RangeIdx,
        data.RangeIdx,
        FormatTimestamp(data.RequestTimestamp).c_str(),
        data.RequestTimestamp,
        FormatTimestamp(data.TestTimestamp).c_str(),
        data.TestTimestamp,
        data.TestId,
        data.TestId,
        data.Checksum,
        data.Checksum);
}

ui64 Checksum(TBlockData blockData)
{
    blockData.Checksum = 0;
    return Crc32c(&blockData, sizeof(blockData));
}

void DumpMap(TFile& file, const TOptions& options)
{
    constexpr ui64 lineBreak = 128;

    TBlockData blockData{};

    ui64 offset = options.StartIndex * options.BlockSize;
    ui64 line = 0;

    char symbol[] = "*.";

    std::printf(" %16" PRIx64 " ", offset);

    for (ui64 i = 0; i != options.BlockCount; ++i) {
        file.Pload(&blockData, sizeof(blockData), offset);
        offset += options.BlockSize;

        const ui64 checksum = Checksum(blockData);

        const bool ok = (checksum == blockData.Checksum) &&
                        (!options.TestId || options.TestId == blockData.TestId);

        std::putchar(symbol[ok]);

        if (++line % lineBreak == 0) {
            std::printf("\n %16" PRIx64 " ", offset);
        }
    }
    std::puts("\n");
}

void DumpText(TFile& file, const TOptions& options)
{
    TBlockData blockData{};

    ui64 offset = options.StartIndex * options.BlockSize;

    const char* status[]{" (corrupted)", ""};

    for (ui64 i = 0; i != options.BlockCount; ++i) {
        file.Pload(&blockData, sizeof(blockData), offset);

        const ui64 checksum = Checksum(blockData);

        std::printf(
            "=========== # %" PRIu64 " 0x%016" PRIx64 "%s ===========\n",
            options.StartIndex + i,
            checksum,
            status[checksum == blockData.Checksum]);

        Dump(blockData);
        std::putchar('\n');

        offset += options.BlockSize;
    }
}

void Run(const TOptions& options)
{
    TFile file{
        options.Path,
        EOpenModeFlag::OpenExisting | EOpenModeFlag::RdOnly};

    switch (options.Format) {
        case EFormat::Text:
            DumpText(file, options);
            break;
        case EFormat::Map:
            DumpMap(file, options);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    try {
        const TOptions options{argc, argv};

        Run(options);

    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
    return 0;
}
