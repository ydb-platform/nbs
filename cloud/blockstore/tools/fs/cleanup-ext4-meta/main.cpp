#include "ext4-meta-reader.h"

#include <cloud/storage/core/libs/common/format.h>

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/folder/path.h>
#include <util/generic/size_literals.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/strip.h>
#include <util/system/file.h>

#include <cstring>
#include <memory>
#include <utility>

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    TString FsPath;
    TString Dumpe2FsOutputPath;
    TString DumpBlockDataDir;

    bool DryRun = false;
    bool Verbose = false;
    bool CleanBitmaps = false;
    bool CleanGDT = false;
    bool CleanSuperblock = false;

    void Parse(int argc, char** argv)
    {
        using namespace NLastGetopt;

        TOpts opts;
        opts.AddHelpOption();

        opts.AddLongOption('f', "filesystem-path")
            .RequiredArgument("FILE")
            .StoreResult(&FsPath);

        opts.AddLongOption('d', "dumpe2fs-output-file")
            .RequiredArgument("FILE")
            .StoreResult(&Dumpe2FsOutputPath);

        opts.AddLongOption("dump-blocks-dir")
            .RequiredArgument("FILE")
            .StoreResult(&DumpBlockDataDir);

        opts.AddLongOption("dry-run").NoArgument().StoreTrue(&DryRun);

        opts.AddLongOption('v', "verbose").NoArgument().StoreTrue(&Verbose);

        opts.AddLongOption("clean-bitmaps")
            .NoArgument()
            .StoreTrue(&CleanBitmaps);

        opts.AddLongOption("clean-gdt").NoArgument().StoreTrue(&CleanGDT);

        opts.AddLongOption("clean-superblock")
            .NoArgument()
            .StoreTrue(&CleanSuperblock);

        bool cleanAll = false;
        opts.AddLongOption("clean-all").NoArgument().StoreTrue(&cleanAll);

        TOptsParseResultException res(&opts, argc, argv);

        if (cleanAll) {
            CleanBitmaps = true;
            CleanGDT = true;
            CleanSuperblock = true;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct IDev
{
    virtual ~IDev() = default;
    virtual void Pload(void* buffer, ui32 byteCount, i64 offset) const = 0;
    virtual void
    Pwrite(const void* buffer, ui32 byteCount, i64 offset) const = 0;
};

struct TDummyDev: IDev
{
    void Pload(void* buffer, ui32 byteCount, i64 offset) const override
    {
        Y_UNUSED(buffer);
        Y_UNUSED(offset);

        memset(buffer, 0xFE, byteCount);
    }

    void Pwrite(const void* buffer, ui32 byteCount, i64 offset) const override
    {
        Y_UNUSED(buffer);
        Y_UNUSED(byteCount);
        Y_UNUSED(offset);
    }
};

struct TVerboseDev: IDev
{
    std::unique_ptr<IDev> Impl;

    explicit TVerboseDev(std::unique_ptr<IDev> impl)
        : Impl(std::move(impl))
    {}

    void Pload(void* buffer, ui32 byteCount, i64 offset) const override
    {
        Cout << "[Pload] " << offset << ":" << byteCount << Endl;
        Impl->Pload(buffer, byteCount, offset);
    }

    void Pwrite(const void* buffer, ui32 byteCount, i64 offset) const override
    {
        Cout << "[Pwrite] " << offset << ":" << byteCount << Endl;
        Impl->Pwrite(buffer, byteCount, offset);
    }
};

struct TDev: IDev
{
    TFile File;

    explicit TDev(const TString& devPath)
        : File(devPath, OpenExisting | RdWr | Sync | DirectAligned)
    {}

    void Pload(void* buffer, ui32 byteCount, i64 offset) const override
    {
        File.Pload(buffer, byteCount, offset);
    }

    void Pwrite(const void* buffer, ui32 byteCount, i64 offset) const override
    {
        File.Pwrite(buffer, byteCount, offset);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TApp
{
private:
    const TOptions& Options;
    IDev& Dev;
    IInputStream& Dumpe2fs;

    char* Buffer;
    char* Zero;
    const ui64 BufferSize;

    TSuperBlock SuperBlock;

    std::unique_ptr<IExt4MetaReader> MetaReader = CreateExt4MetaReader();

    ui64 TrimmedBlocks = 0;
    ui64 TrimmedBytes = 0;

public:
    TApp(const TOptions& options, IDev& dev, IInputStream& dumpe2fs)
        : Options(options)
        , Dev(dev)
        , Dumpe2fs(dumpe2fs)
        , Buffer(static_cast<char*>(std::aligned_alloc(4_KB, 4_MB)))
        , Zero(static_cast<char*>(std::aligned_alloc(4_KB, 4_MB)))
        , BufferSize(4_MB)
    {
        std::memset(Zero, 0, BufferSize);
    }

    ~TApp()
    {
        std::free(Zero);
        std::free(Buffer);
    }

    int Run()
    {
        SuperBlock = MetaReader->ReadSuperBlock(Dumpe2fs);

        if (Options.Verbose) {
            Cout << "SuperBlock: " << SuperBlock.BlockSize << " "
                 << SuperBlock.BlocksPerGroup << " "
                 << SuperBlock.InodesPerGroup << " "
                 << SuperBlock.MetadataCsumFeature << Endl;
        }

        Y_ABORT_UNLESS(SuperBlock.BlockSize == 4_KB);

        while (auto group = MetaReader->ReadGroupDescr(Dumpe2fs)) {
            ProcessGroup(*group);
        }

        if (Options.Verbose) {
            Cout << "Trimmed blocks: " << TrimmedBlocks << Endl;
            Cout << "Trimmed bytes: " << TrimmedBytes << " ("
                 << NCloud::FormatByteSize(TrimmedBytes) << ")" << Endl;
        }

        return 0;
    }

private:
    void ProcessGroup(const TGroupDescr& groupDescr)
    {
        if (Options.Verbose) {
            Cout << "Process group " << groupDescr.GroupNum << " (blocks "
                 << groupDescr.Blocks.FirstBlock << "-"
                 << groupDescr.Blocks.LastBlock << ") ..." << Endl;
        }

        ui64 totalBlocks = groupDescr.Blocks.Length();

        if (groupDescr.GroupDescriptors) {
            const ui64 len = groupDescr.GroupDescriptors->Length();
            Y_ABORT_UNLESS(totalBlocks >= len);

            totalBlocks -= len;
        }

        if (groupDescr.ReservedGDTBlocks) {
            const ui64 len = groupDescr.ReservedGDTBlocks->Length();
            Y_ABORT_UNLESS(totalBlocks >= len);

            totalBlocks -= len;

            if (Options.CleanGDT && groupDescr.GroupNum != 0) {
                CleanupReservedGDTBlocks(*groupDescr.ReservedGDTBlocks);
            }
        }

        if (groupDescr.PrimarySuperblock) {
            Y_ABORT_UNLESS(totalBlocks);
            totalBlocks -= 1;

            if (Options.CleanSuperblock) {
                CleanupPrimarySB(*groupDescr.PrimarySuperblock);
            }
        }

        if (groupDescr.BackupSuperblock) {
            Y_ABORT_UNLESS(totalBlocks);
            totalBlocks -= 1;

            if (Options.CleanSuperblock) {
                CleanupBackupSB(*groupDescr.BackupSuperblock);
            }
        }

        if (Options.CleanBitmaps) {
            CleanupBlockBitmap(groupDescr, totalBlocks);
            CleanupInodeBitmap(groupDescr);
        }
    }

    void CleanupBlockBitmap(const TGroupDescr& groupDescr, ui64 totalBlocks)
    {
        if (groupDescr.FreeBlocks != SuperBlock.BlocksPerGroup) {
            return;
        }

        if (groupDescr.BlockBitmap == 0) {
            Cerr << "WARN [CleanupBlockBitmap] empty BlockBitmap in Group #"
                 << groupDescr.GroupNum << Endl;
            return;
        }

        if (groupDescr.FreeBlocks != totalBlocks) {
            Cerr << "WARN [CleanupBlockBitmap] " << groupDescr.FreeBlocks
                 << " != " << totalBlocks << " for Group #"
                 << groupDescr.GroupNum << Endl;
            return;
        }

        const i64 offset =
            static_cast<i64>(groupDescr.BlockBitmap * SuperBlock.BlockSize);

        if (Options.DumpBlockDataDir) {
            Dev.Pload(Buffer, SuperBlock.BlockSize, offset);
            DumpBlock(groupDescr.BlockBitmap, Buffer);
        }

        Dev.Pwrite(Zero, SuperBlock.BlockSize, offset);

        ++TrimmedBlocks;
        TrimmedBytes += SuperBlock.BlockSize;
    }

    void CleanupInodeBitmap(const TGroupDescr& groupDescr)
    {
        if (groupDescr.FreeInodes != SuperBlock.InodesPerGroup) {
            return;
        }

        if (groupDescr.InodeBitmap == 0) {
            Cerr << "WARN [CleanupInodeBitmap] empty InodeBitmap in Group #"
                 << groupDescr.GroupNum << Endl;
            return;
        }

        if (groupDescr.UnusedInodes != groupDescr.FreeInodes) {
            Cerr << "WARN [CleanupInodeBitmap] " << groupDescr.UnusedInodes
                 << " != " << groupDescr.FreeInodes << " for Group #"
                 << groupDescr.GroupNum << Endl;
            return;
        }

        const i64 offset =
            static_cast<i64>(groupDescr.InodeBitmap * SuperBlock.BlockSize);

        if (Options.DumpBlockDataDir) {
            Dev.Pload(Buffer, SuperBlock.BlockSize, offset);
            DumpBlock(groupDescr.InodeBitmap, Buffer);
        }

        Dev.Pwrite(Zero, SuperBlock.BlockSize, offset);

        ++TrimmedBlocks;
        TrimmedBytes += SuperBlock.BlockSize;
    }

    void DumpBlock(ui64 blockIndex, const char* data)
    {
        auto filename =
            JoinFsPaths(Options.DumpBlockDataDir, ToString(blockIndex));

        TFile file(
            filename,
            EOpenModeFlag::CreateAlways | EOpenModeFlag::WrOnly);
        file.Write(data, SuperBlock.BlockSize);
    }

    void CleanupPrimarySB(ui64 blockIndex)
    {
        Y_ABORT_UNLESS(SuperBlock.BlockSize == 4_KB);

        const i64 offset = static_cast<i64>(blockIndex * SuperBlock.BlockSize);

        Dev.Pload(Buffer, 4_KB, offset);
        if (Options.DumpBlockDataDir) {
            DumpBlock(blockIndex, Buffer);
        }

        memset(Buffer, 0, 1_KB);
        TrimmedBytes += 1_KB;

        memset(Buffer + 2_KB, 0, 2_KB);
        TrimmedBytes += 2_KB;

        Dev.Pwrite(Buffer, 4_KB, offset);
    }

    void CleanupBackupSB(ui64 blockIndex)
    {
        const i64 offset = static_cast<i64>(blockIndex * SuperBlock.BlockSize);

        Dev.Pload(Buffer, 4_KB, offset);
        if (Options.DumpBlockDataDir) {
            DumpBlock(blockIndex, Buffer);
        }

        memset(Buffer + 1_KB, 0, 3_KB);
        TrimmedBytes += 3_KB;

        Dev.Pwrite(Buffer, 4_KB, offset);
    }

    void CleanupReservedGDTBlocks(TBlockRange reservedGDTBlocks)
    {
        auto [startIndex, endIndex] = reservedGDTBlocks;

        const ui64 blocksInBuffer = BufferSize / SuperBlock.BlockSize;

        while (startIndex < endIndex) {
            const i64 offset =
                static_cast<i64>(startIndex * SuperBlock.BlockSize);
            const ui64 blocksToRead =
                std::min(blocksInBuffer, endIndex - startIndex + 1);
            const ui32 bytesToRead = blocksToRead * SuperBlock.BlockSize;

            Dev.Pload(Buffer, bytesToRead, offset);

            if (Options.DumpBlockDataDir) {
                auto filename = JoinFsPaths(
                    Options.DumpBlockDataDir,
                    TStringBuilder() << startIndex << "-" << blocksToRead);

                TFile file(
                    filename,
                    EOpenModeFlag::CreateAlways | EOpenModeFlag::WrOnly);
                file.Write(Buffer, bytesToRead);
            }

            Dev.Pwrite(Zero, bytesToRead, offset);

            TrimmedBytes += bytesToRead;
            TrimmedBlocks += blocksToRead;

            startIndex += blocksToRead;
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

int Run(const TOptions& options, IDev& dev, IInputStream& dumpe2fs)
{
    TApp app(options, dev, dumpe2fs);

    return app.Run();
}

auto CreateDev(const TOptions& options)
{
    std::unique_ptr<IDev> dev;

    if (options.DryRun) {
        dev = std::make_unique<TDummyDev>();
    } else {
        dev = std::make_unique<TDev>(options.FsPath);
    }

    if (options.Verbose) {
        dev = std::make_unique<TVerboseDev>(std::move(dev));
    }

    return dev;
}

int main(int argc, char** argv)
{
    TOptions options;
    options.Parse(argc, argv);

    std::unique_ptr<IDev> dev = CreateDev(options);

    if (!options.Dumpe2FsOutputPath.empty()) {
        TFileInput file{options.Dumpe2FsOutputPath};

        return Run(options, *dev, file);
    }

    return Run(options, *dev, Cin);
}
