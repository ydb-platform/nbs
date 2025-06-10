#include "lowlevel.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/generic/buffer.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLowlevelTest)
{
    Y_UNIT_TEST(ShouldOpenOrCreateFile)
    {
        const TTempDir TempDir;
        auto node = NLowLevel::Open(TempDir.Name(), O_PATH, 0);

        // WasCreated should be true or false based on whether file already
        // exist or not
        auto res =
            NLowLevel::OpenOrCreateAt(node, "1.txt", O_CREAT | O_WRONLY, 0755);
        UNIT_ASSERT(res.WasCreated);

        TString expectedData = "abdef";
        res.Handle.Write(
            const_cast<char*>(expectedData.c_str()),
            expectedData.size());

        res =
            NLowLevel::OpenOrCreateAt(node, "1.txt", O_CREAT | O_RDONLY, 0755);
        UNIT_ASSERT(!res.WasCreated);
        res.Handle.Flush();

        char buf[256] = {};
        UNIT_ASSERT_EQUAL(
            static_cast<i32>(expectedData.size()),
            res.Handle.Read(buf, expectedData.size()));

        // O_EXCL should still behave properly
        res = NLowLevel::OpenOrCreateAt(
            node,
            "2.txt",
            O_CREAT | O_EXCL | O_WRONLY,
            0755);
        UNIT_ASSERT(res.WasCreated);

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            NLowLevel::OpenOrCreateAt(
                node,
                "2.txt",
                O_CREAT | O_EXCL | O_WRONLY,
                0755),
            yexception,
            "File exists");

        // Just open existing file also works
        res = NLowLevel::OpenOrCreateAt(node, "2.txt", O_WRONLY, 0755);
        UNIT_ASSERT(!res.WasCreated);
    }

    Y_UNIT_TEST(ShouldDoIterativeListDir)
    {
        const TTempDir TempDir;
        auto rootNode = NLowLevel::Open(TempDir.Name(), O_PATH, 0);

        int nodesCount = 10;
        TSet<TString> entryNames;
        for (int i = 0; i < nodesCount; i++) {
            if (i == 0) {
                auto name = "dir_" + ToString(i);
                entryNames.insert(name);
                NLowLevel::MkDirAt(rootNode, name, 0755);
            } else {
                auto name = "file_" + ToString(i);
                entryNames.insert(name);
                NLowLevel::OpenAt(
                    rootNode,
                    name,
                    O_CREAT | O_WRONLY,
                    0755);
            }
        }

        auto checkListDirResult =
            [&](NLowLevel::TListDirResult& res, size_t expectedEntriesCount)
        {
            UNIT_ASSERT_EQUAL(res.DirEntries.size(), expectedEntriesCount);
            for (auto& entry: res.DirEntries) {
                UNIT_ASSERT_EQUAL_C(
                    1,
                    entryNames.count(entry.first),
                    TStringBuilder() << entry.first << " missing");
                entryNames.erase(entry.first);
            }
        };

        auto res = NLowLevel::ListDirAt(rootNode, 0, 5, false);
        checkListDirResult(res, 5);

        res = NLowLevel::ListDirAt(rootNode, res.DirOffset, 3, false);
        checkListDirResult(res, 3);

        res = NLowLevel::ListDirAt(rootNode, res.DirOffset, 2, false);
        checkListDirResult(res, 2);

        res = NLowLevel::ListDirAt(rootNode, res.DirOffset, 2, false);
        checkListDirResult(res, 0);
    }
};

}   // namespace NCloud::NFileStore
