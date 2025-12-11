#include "ext4-meta-reader.h"

#include <library/cpp/testing/unittest/env.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/stream/file.h>

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TExt4MetaReaderTest)
{
    Y_UNIT_TEST(ShouldParseEmptyExt4)
    {
        auto reader = CreateExt4MetaReader();

        TFileInput file(JoinFsPaths(GetWorkPath(), "empty-ext4.txt"));

        const auto sb = reader->ReadSuperBlock(file);

        UNIT_ASSERT(sb.MetadataCsumFeature);
        UNIT_ASSERT_VALUES_EQUAL(4096, sb.BlockSize);
        UNIT_ASSERT_VALUES_EQUAL(8192, sb.InodesPerGroup);
        UNIT_ASSERT_VALUES_EQUAL(32768, sb.BlocksPerGroup);

        ui32 groupCount = 0;
        ui64 offset = 0;

        while (auto group = reader->ReadGroupDescr(file)) {
            UNIT_ASSERT_VALUES_EQUAL(groupCount, group->GroupNum);
            UNIT_ASSERT_VALUES_EQUAL(offset, group->Blocks.FirstBlock);
            UNIT_ASSERT_VALUES_EQUAL(
                offset + sb.BlocksPerGroup - 1,
                group->Blocks.LastBlock);

            offset += sb.BlocksPerGroup;

            switch (group->GroupNum) {
                case 0:
                    UNIT_ASSERT(group->PrimarySuperblock);
                    UNIT_ASSERT_VALUES_EQUAL(0, *group->PrimarySuperblock);

                    UNIT_ASSERT(group->GroupDescriptors);
                    UNIT_ASSERT_VALUES_EQUAL(
                        1,
                        group->GroupDescriptors->FirstBlock);
                    UNIT_ASSERT_VALUES_EQUAL(
                        12,
                        group->GroupDescriptors->LastBlock);

                    UNIT_ASSERT(group->ReservedGDTBlocks);
                    UNIT_ASSERT_VALUES_EQUAL(
                        13,
                        group->ReservedGDTBlocks->FirstBlock);
                    UNIT_ASSERT_VALUES_EQUAL(
                        1036,
                        group->ReservedGDTBlocks->LastBlock);

                    UNIT_ASSERT_VALUES_UNEQUAL(0, group->BlockBitmapCS);
                    UNIT_ASSERT_VALUES_UNEQUAL(0, group->InodeBitmapCS);

                    break;
                case 1:
                case 3:
                case 5:
                case 7:
                case 9:
                case 25:
                case 27:
                case 49:
                case 81:
                case 125:
                case 243:
                case 343:
                case 625:
                case 729:
                    UNIT_ASSERT(group->BackupSuperblock);
                    UNIT_ASSERT_VALUES_EQUAL(
                        group->GroupNum * sb.BlocksPerGroup,
                        *group->BackupSuperblock);

                    UNIT_ASSERT(group->GroupDescriptors);
                    UNIT_ASSERT_VALUES_EQUAL(
                        *group->BackupSuperblock + 1,
                        group->GroupDescriptors->FirstBlock);

                    UNIT_ASSERT(group->ReservedGDTBlocks);
                    UNIT_ASSERT_VALUES_EQUAL(
                        group->GroupDescriptors->LastBlock + 1,
                        group->ReservedGDTBlocks->FirstBlock);

                    UNIT_ASSERT_VALUES_EQUAL(0, group->BlockBitmapCS);
                    UNIT_ASSERT_VALUES_EQUAL(0, group->InodeBitmapCS);

                    break;
                default:
                    UNIT_ASSERT(!group->PrimarySuperblock);
                    UNIT_ASSERT(!group->BackupSuperblock);
                    UNIT_ASSERT(!group->GroupDescriptors);
                    UNIT_ASSERT(!group->ReservedGDTBlocks);

                    break;
            }

            switch (group->GroupNum) {
                case 0:
                    UNIT_ASSERT_VALUES_UNEQUAL(0, group->BlockBitmapCS);
                    UNIT_ASSERT_VALUES_UNEQUAL(0, group->InodeBitmapCS);
                    break;

                case 16:
                case 32:
                case 48:
                case 64:
                case 80:
                case 96:
                case 112:
                case 128:
                case 144:
                case 160:
                case 176:
                case 192:
                case 208:
                case 224:
                case 240:
                case 256:
                case 272:
                case 288:
                case 304:
                case 320:
                case 336:
                case 352:
                case 368:
                case 369:
                case 370:
                case 371:
                case 372:
                case 384:
                case 400:
                case 416:
                case 432:
                case 448:
                case 464:
                case 480:
                case 496:
                case 512:
                case 528:
                case 544:
                case 560:
                case 576:
                case 592:
                case 608:
                case 624:
                case 640:
                case 656:
                case 672:
                case 688:
                case 704:
                case 720:
                case 736:
                case 743:
                    UNIT_ASSERT_VALUES_UNEQUAL(0, group->BlockBitmapCS);
                    UNIT_ASSERT_VALUES_EQUAL(0, group->InodeBitmapCS);
                    break;
                default:
                    UNIT_ASSERT_VALUES_EQUAL(0, group->BlockBitmapCS);
                    UNIT_ASSERT_VALUES_EQUAL(0, group->InodeBitmapCS);
                    break;
            }

            ++groupCount;
        }
        UNIT_ASSERT_VALUES_EQUAL(744, groupCount);
        UNIT_ASSERT_VALUES_EQUAL(24379392, offset);
    }
}
