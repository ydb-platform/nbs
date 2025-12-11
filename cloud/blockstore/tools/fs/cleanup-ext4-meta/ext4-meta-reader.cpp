#include "ext4-meta-reader.h"

#include <library/cpp/regex/pcre/pcre.h>

#include <util/string/cast.h>
#include <util/string/strip.h>

#include <memory>

namespace {

////////////////////////////////////////////////////////////////////////////////

TStringBuf FromMatch(TStringBuf orig, NPcre::TPcreMatch m)
{
    return orig.substr(m.first, m.second - m.first);
}

template <typename T>
T FromMatch(TStringBuf orig, NPcre::TPcreMatch m)
{
    return FromString<T>(FromMatch(orig, m));
}

template <typename T, int B>
T IntFromMatch(TStringBuf orig, NPcre::TPcreMatch m)
{
    return IntFromString<T, B>(FromMatch(orig, m));
}

NPcre::TPcre<char> CreateRE(const char* s)
{
    return NPcre::TPcre<char>{s, NPcre::EOptimize::JIT};
}

////////////////////////////////////////////////////////////////////////////////

class TExt4MetaReader final: public IExt4MetaReader
{
private:
    const NPcre::TPcre<char> GroupRE =
        CreateRE(R"(Group (\d+): \(Blocks (\d+)\-(\d+)\))");

    // Primary superblock at 0, Group descriptors at 1-12
    const NPcre::TPcre<char> PrimarySuperblockRegExp = CreateRE(
        R"(Primary superblock at (\d+), Group descriptors at (\d+)\-(\d+))");

    // Backup superblock at 23887872, Group descriptors at 23887873-23887884
    const NPcre::TPcre<char> BackupSuperblockRegExp = CreateRE(
        R"(Backup superblock at (\d+), Group descriptors at (\d+)\-(\d+))");

    const NPcre::TPcre<char> ReservedGDTBlocksRegExp =
        CreateRE(R"(Reserved GDT blocks at (\d+)\-(\d+))");

    // Block bitmap at 20447233 (bg #624 + 1)(, csum 0x00000000)?
    // Block bitmap at 1037 (+1037), csum 0xf64b0223
    const NPcre::TPcre<char> BlockBitmapRegExp =
        CreateRE(R"(Block bitmap at (\d+) \([^)]+\)(, csum 0x([a-z0-9]+))?)");

    // Inode bitmap at 20447249 (bg #624 + 17)(, csum 0x00000000)?
    const NPcre::TPcre<char> InodeBitmapRegExp =
        CreateRE(R"(Inode bitmap at (\d+) \([^)]+\)(, csum 0x([0-9a-z]+))?)");

    const NPcre::TPcre<char> FreeBlocksRegExp = CreateRE(
        R"((\d+) free blocks, (\d+) free inodes, \d+ directories, (\d+) unused inodes)");

public:
    TSuperBlock ReadSuperBlock(IInputStream& stream)
    {
        TSuperBlock sb;

        int blanks = 0;

        auto blockSizeRE = CreateRE(R"(Block size:\s+(\d+))");
        auto blocksPerGroup = CreateRE(R"(Blocks per group:\s+(\d+))");
        auto inodesPerGroup = CreateRE(R"(Inodes per group:\s+(\d+))");

        TString line;
        while (stream.ReadLine(line)) {
            if (line.empty()) {
                ++blanks;

                if (blanks == 2) {
                    break;
                }
            }

            if (auto match = blockSizeRE.Capture(line); !match.empty()) {
                sb.BlockSize = FromMatch<ui32>(line, match[1]);
                continue;
            }

            if (auto match = blocksPerGroup.Capture(line); !match.empty()) {
                sb.BlocksPerGroup = FromMatch<ui32>(line, match[1]);
                continue;
            }

            if (auto match = inodesPerGroup.Capture(line); !match.empty()) {
                sb.InodesPerGroup = FromMatch<ui32>(line, match[1]);
                continue;
            }

            if (line.StartsWith("Filesystem features") &&
                line.Contains("metadata_csum"))
            {
                sb.MetadataCsumFeature = true;
            }
        }

        return sb;
    }

    TMaybe<TGroupDescr> ReadGroupDescr(IInputStream& stream)
    {
        TGroupDescr groupDescr;

        TString line;
        if (!stream.ReadLine(line)) {
            return {};
        }

        if (auto match = GroupRE.Capture(line); !match.empty()) {
            groupDescr.GroupNum = FromMatch<ui32>(line, match[1]);
            groupDescr.Blocks = {
                FromMatch<ui64>(line, match[2]),
                FromMatch<ui64>(line, match[3])};
        } else {
            Y_ABORT_UNLESS(false, "can't parse group info: %s", line.c_str());
        }

        while (stream.ReadLine(line)) {
            line = StripStringLeft(line);
            if (line.StartsWith("Free inodes")) {   // end of group
                break;
            }

            if (auto match = FreeBlocksRegExp.Capture(line); !match.empty()) {
                Y_ABORT_UNLESS(!groupDescr.FreeBlocks);
                Y_ABORT_UNLESS(!groupDescr.FreeInodes);
                Y_ABORT_UNLESS(!groupDescr.UnusedInodes);

                groupDescr.FreeBlocks = FromMatch<ui64>(line, match[1]);
                groupDescr.FreeInodes = FromMatch<ui64>(line, match[2]);
                groupDescr.UnusedInodes = FromMatch<ui64>(line, match[3]);
            }

            if (auto match = BlockBitmapRegExp.Capture(line); !match.empty()) {
                Y_ABORT_UNLESS(!groupDescr.BlockBitmap);
                Y_ABORT_UNLESS(!groupDescr.BlockBitmapCS);

                groupDescr.BlockBitmap = FromMatch<ui64>(line, match[1]);
                if (match.size() > 3) {
                    groupDescr.BlockBitmapCS =
                        IntFromMatch<ui64, 16>(line, match[3]);
                } else {
                    groupDescr.BlockBitmapCS = -1;
                }
                continue;
            }

            if (auto match = InodeBitmapRegExp.Capture(line); !match.empty()) {
                Y_ABORT_UNLESS(!groupDescr.InodeBitmap);
                Y_ABORT_UNLESS(!groupDescr.InodeBitmapCS);

                groupDescr.InodeBitmap = FromMatch<ui64>(line, match[1]);
                if (match.size() > 3) {
                    groupDescr.InodeBitmapCS =
                        IntFromMatch<ui64, 16>(line, match[3]);
                } else {
                    groupDescr.InodeBitmapCS = -1;
                }
                continue;
            }

            if (auto match = PrimarySuperblockRegExp.Capture(line);
                !match.empty())
            {
                Y_ABORT_UNLESS(!groupDescr.PrimarySuperblock);
                Y_ABORT_UNLESS(!groupDescr.GroupDescriptors);

                groupDescr.PrimarySuperblock = FromMatch<ui64>(line, match[1]);

                groupDescr.GroupDescriptors = TBlockRange{
                    FromMatch<ui64>(line, match[2]),
                    FromMatch<ui64>(line, match[3])};

                continue;
            }

            if (auto match = BackupSuperblockRegExp.Capture(line);
                !match.empty())
            {
                Y_ABORT_UNLESS(!groupDescr.BackupSuperblock);
                Y_ABORT_UNLESS(!groupDescr.GroupDescriptors);

                groupDescr.BackupSuperblock = FromMatch<ui64>(line, match[1]);

                groupDescr.GroupDescriptors = TBlockRange{
                    FromMatch<ui64>(line, match[2]),
                    FromMatch<ui64>(line, match[3])};

                continue;
            }

            if (auto match = ReservedGDTBlocksRegExp.Capture(line);
                !match.empty())
            {
                Y_ABORT_UNLESS(!groupDescr.ReservedGDTBlocks);

                groupDescr.ReservedGDTBlocks = TBlockRange{
                    FromMatch<ui64>(line, match[1]),
                    FromMatch<ui64>(line, match[2])};

                continue;
            }
        }

        return groupDescr;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IExt4MetaReader> CreateExt4MetaReader()
{
    return std::make_unique<TExt4MetaReader>();
}
