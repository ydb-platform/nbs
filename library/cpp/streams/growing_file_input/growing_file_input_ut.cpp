#include "growing_file_input.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/buffer.h>
#include <util/stream/file.h>
#include <util/thread/factory.h>
#include <util/system/tempfile.h>

static const char* TmpFileName = "./fileio";
static const char* TmpFileOldContents = "This text was in file";
static const char* TmpFileNewContents = "This text appended to file";

namespace {
    void ReadTail(TGrowingFileInput& input) {
        size_t size = strlen(TmpFileNewContents);
        TBuffer buffer(size + 1);
        input.Read(buffer.data(), size);

        (buffer.data())[size] = '\0';

        UNIT_ASSERT(!strcmp(buffer.data(), TmpFileNewContents));
    }

    void WriteTail() {
        TUnbufferedFileOutput output(TFile::ForAppend(TmpFileName));
        output.Write(TmpFileNewContents, strlen(TmpFileNewContents));
    }

}

Y_UNIT_TEST_SUITE(TGrowingFileInputTest) {
    Y_UNIT_TEST(InputTest) {
        TTempFile tmp(TmpFileName);

        {
            TUnbufferedFileOutput output(TmpFileName);
            output.Write(TmpFileOldContents, strlen(TmpFileOldContents));
        }

        TGrowingFileInput input(TmpFileName);

        THolder<IThreadFactory::IThread> tmpThread1 = SystemThreadFactory()->Run([&input]() { ReadTail(input); });
        THolder<IThreadFactory::IThread> tmpThread2 = SystemThreadFactory()->Run(WriteTail);

        tmpThread1->Join();
        tmpThread2->Join();
    }
}
