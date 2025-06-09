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
            static_cast<i32>(expectedData.Size()),
            res.Handle.Read(buf, expectedData.Size()));

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
};

}   // namespace NCloud::NFileStore
