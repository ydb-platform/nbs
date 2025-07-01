#include "lowlevel.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/generic/buffer.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLowlevelTest)
{
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

        auto savedEntryNames = entryNames;
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

        // with limit 0 all nodes should be read
        entryNames = savedEntryNames;
        res = NLowLevel::ListDirAt(rootNode, 0, 0, false);
        checkListDirResult(res, 10);

    }
};

}   // namespace NCloud::NFileStore
