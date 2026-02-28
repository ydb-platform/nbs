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

        builder.Add(
            req,
            ".",
            {.attr = {.st_ino = MissingNodeId}},
            offset);
        builder.Add(
            req,
            "..",
            {.attr = {.st_ino = MissingNodeId}},
            offset);

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

        {
            fuse_entry_param entry = {
                .ino = 10003,
                .attr_timeout = attrTimeout,
                .entry_timeout = entryTimeout,
            };

            NProto::TNodeAttr attr;
            attr.SetId(10003);
            attr.SetType(NProto::E_DIRECTORY_NODE);
            ConvertAttr(preferredBlockSize, attr, entry.attr);

            builder.Add(req, "dir2", entry, offset);
        }

        auto buffer = builder.Finish();

        auto validateEntry = [&] (
            ui64 attrTimeoutOverride,
            ui64 entryTimeoutOverride,
            TStringBuf& buf,
            ui64 ino,
            TStringBuf fileName,
            ui32 entryType,
            ui64 size)
        {
            UNIT_ASSERT_GT(buf.Size(), sizeof(fuse_direntplus));

            const auto* de =
                reinterpret_cast<const fuse_direntplus*>(buf.data());

            UNIT_ASSERT_VALUES_EQUAL(ino, de->dirent.ino);
            TStringBuf name(de->dirent.name, de->dirent.namelen);
            UNIT_ASSERT_VALUES_EQUAL(fileName, name);
            UNIT_ASSERT_VALUES_EQUAL(entryType, de->entry_out.attr.mode);
            UNIT_ASSERT_VALUES_EQUAL(size, de->entry_out.attr.size);
            UNIT_ASSERT_VALUES_EQUAL(
                attrTimeoutOverride,
                de->entry_out.attr_valid);
            UNIT_ASSERT_VALUES_EQUAL(0, de->entry_out.attr_valid_nsec);
            UNIT_ASSERT_VALUES_EQUAL(
                entryTimeoutOverride,
                de->entry_out.entry_valid);
            UNIT_ASSERT_VALUES_EQUAL(0, de->entry_out.entry_valid_nsec);

            const ui64 nameLen =
                AlignUp<ui64>(de->dirent.namelen, sizeof(ui64));
            buf.Skip(sizeof(*de) + nameLen);
        };

        auto validate = [&] (ui64 attrTimeoutOverride) {
            auto buf = TStringBuf(buffer->Data(), buffer->Size());

            validateEntry(0, 0, buf, MissingNodeId, ".", 0, 0);
            validateEntry(0, 0, buf, MissingNodeId, "..", 0, 0);
            validateEntry(
                attrTimeoutOverride,
                entryTimeout,
                buf,
                10001,
                "file1",
                S_IFREG,
                10_KB);
            validateEntry(
                attrTimeoutOverride,
                entryTimeout,
                buf,
                10002,
                "dir1",
                S_IFDIR,
                0);
            validateEntry(
                attrTimeout,
                entryTimeout,
                buf,
                10003,
                "dir2",
                S_IFDIR,
                0);
        };

        validate(attrTimeout);
        auto error = ResetAttrTimeout(
            buffer->Data(),
            buffer->Size(),
            [] (ui64 ino) {
                return ino != 10003 && ino != MissingNodeId;
            });
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));
        validate(0);
    }

    Y_UNIT_TEST(ShouldDetectNodeIdMismatch)
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
            attr.SetId(10003);
            attr.SetType(NProto::E_REGULAR_NODE);
            attr.SetSize(10_KB);
            ConvertAttr(preferredBlockSize, attr, entry.attr);

            builder.Add(req, "file1", entry, offset);
        }

        auto buffer = builder.Finish();

        auto error = ResetAttrTimeout(
            buffer->Data(),
            buffer->Size(),
            [] (ui64 ino) {
                Y_UNUSED(ino);
                return true;
            });
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_INVALID_STATE,
            error.GetCode(),
            FormatError(error));
    }

    Y_UNIT_TEST(ShouldDetectEmptyName)
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

            builder.Add(req, "", entry, offset);
        }

        auto buffer = builder.Finish();

        auto error = ResetAttrTimeout(
            buffer->Data(),
            buffer->Size(),
            [] (ui64 ino) {
                Y_UNUSED(ino);
                return true;
            });
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_INVALID_STATE,
            error.GetCode(),
            FormatError(error));
    }

    Y_UNIT_TEST(ShouldDetectGarbage)
    {
        TString garbage(200, 0);
        for (ui32 i = 0; i < garbage.Size(); ++i) {
            garbage[i] = i;
        }

        auto error = ResetAttrTimeout(
            garbage.begin(),
            garbage.size(),
            [] (ui64 ino) {
                Y_UNUSED(ino);
                return true;
            });
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_INVALID_STATE,
            error.GetCode(),
            FormatError(error));
    }
}

}   // namespace NCloud::NFileStore::NFuse
