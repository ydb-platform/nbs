#include "starter.h"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size)
{
    using namespace NCloud::NFileStore::NFuse;

    if (auto* starter = TStarter::GetStarter()) {
        return starter->Run(data, size);
    }

    return 0;
}
