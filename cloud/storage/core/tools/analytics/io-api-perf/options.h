#include <util/datetime/base.h>
#include <util/generic/size_literals.h>
#include <util/generic/string.h>

////////////////////////////////////////////////////////////////////////////////

enum EApi
{
    IoUring,
    LinuxAio,
    NbsAio,

    TestAll
};

TString ToString(EApi api);

struct TOptions
{
    EApi Api = EApi::TestAll;

    TString Path;
    TDuration Duration = TDuration::Minutes(1);
    ui32 RingSize = 1024;
    ui32 IoDepth = 1;
    ui32 BlockSize = 4_KB;
    ui32 Threads = 1;

    ui32 NbsAioThreads = 0;

    bool Quiet = false;
    bool SqPolling = false;

    TOptions(int argc, const char** argv);
};
