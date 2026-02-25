#include "fs_directory_content_format.h"

#include <cloud/filestore/libs/vfs/convert.h>
#include <cloud/contrib/virtiofsd/fuse.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/align.h>

namespace NCloud::NFileStore::NFuse {

using namespace NCloud::NFileStore::NVFS;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDirectoryContentFormatTest)
{
    Y_UNIT_TEST(ShouldBuildContent)
    {
        const ui64 size = 4_KB;
        const ui64 attrTimeout = 15;
        const ui64 entryTimeout = 10;
        const ui32 preferredBlockSize = 4_KB;
        const size_t offset = 0;
        fuse_req_t req = nullptr;

        TDirectoryBuilder builder(size);

        {
            fuse_entry_param entry = {
                .ino = 10001,
                .attr_timeout = attrTimeout,
                .entry_timeout = entryTimeout,
            };

            NProto::TNodeAttr attr;
            attr.SetId(10001);
            attr.SetType(NProto::E_REGULAR_NODE);
            attr.SetSize(10_KB);
            ConvertAttr(preferredBlockSize, attr, entry.attr);

            builder.Add(req, "file1", entry, offset);
        }

        {
            fuse_entry_param entry = {
                .ino = 10002,
                .attr_timeout = attrTimeout,
                .entry_timeout = entryTimeout,
            };

            NProto::TNodeAttr attr;
            attr.SetId(10002);
            attr.SetType(NProto::E_DIRECTORY_NODE);
            ConvertAttr(preferredBlockSize, attr, entry.attr);

            builder.Add(req, "dir1", entry, offset);
        }

        auto buffer = builder.Finish();

        auto buf = TStringBuf(buffer->Data(), buffer->Size());
        UNIT_ASSERT_GT(buf.Size(), sizeof(fuse_direntplus));

        {
            const auto* de =
                reinterpret_cast<const fuse_direntplus*>(buf.data());

            UNIT_ASSERT_VALUES_EQUAL(10001, de->dirent.ino);
            TStringBuf name(de->dirent.name, de->dirent.namelen);
            UNIT_ASSERT_VALUES_EQUAL("file1", name);
            UNIT_ASSERT_VALUES_EQUAL(S_IFREG, de->entry_out.attr.mode);
            UNIT_ASSERT_VALUES_EQUAL(10_KB, de->entry_out.attr.size);
            UNIT_ASSERT_VALUES_EQUAL(attrTimeout, de->entry_out.attr_valid);
            UNIT_ASSERT_VALUES_EQUAL(0, de->entry_out.attr_valid_nsec);
            UNIT_ASSERT_VALUES_EQUAL(entryTimeout, de->entry_out.entry_valid);
            UNIT_ASSERT_VALUES_EQUAL(0, de->entry_out.entry_valid_nsec);

            buf.Skip(
                sizeof(*de) + AlignUp<ui64>(de->dirent.namelen, sizeof(ui64)));
        }

        UNIT_ASSERT_GT(buf.Size(), sizeof(fuse_direntplus));

        {
            const auto* de =
                reinterpret_cast<const fuse_direntplus*>(buf.data());

            UNIT_ASSERT_VALUES_EQUAL(10002, de->dirent.ino);
            TStringBuf name(de->dirent.name, de->dirent.namelen);
            UNIT_ASSERT_VALUES_EQUAL("dir1", name);
            UNIT_ASSERT_VALUES_EQUAL(S_IFDIR, de->entry_out.attr.mode);
            UNIT_ASSERT_VALUES_EQUAL(0, de->entry_out.attr.size);
            UNIT_ASSERT_VALUES_EQUAL(attrTimeout, de->entry_out.attr_valid);
            UNIT_ASSERT_VALUES_EQUAL(0, de->entry_out.attr_valid_nsec);
            UNIT_ASSERT_VALUES_EQUAL(entryTimeout, de->entry_out.entry_valid);
            UNIT_ASSERT_VALUES_EQUAL(0, de->entry_out.entry_valid_nsec);

            buf.Skip(
                sizeof(*de) + AlignUp<ui64>(de->dirent.namelen, sizeof(ui64)));
        }

        UNIT_ASSERT_VALUES_EQUAL(0, buf.Size());
    }
}

}   // namespace NCloud::NFileStore::NFuse
