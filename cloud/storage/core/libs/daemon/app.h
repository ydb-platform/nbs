#pragma once

#include "public.h"

#include <util/generic/yexception.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

void AppCreate();
int AppMain(TProgramShouldContinue& shouldContinue);
void AppStop();

void ConfigureSignals();

template <typename TBootstrap>
int DoMain(TBootstrap& bootstrap, int argc, char** argv)
{
    try {
        // To prevent call to placement new or delete inside signal handler
        // before creating App singleton. This behaviour is triggered by our
        // nemesis tests with ThreadSanitizer.
        AppCreate();

        ConfigureSignals();

        try {
            bootstrap.ParseOptions(argc, argv);
        } catch (...) {
            Cerr << "main options parse: " << CurrentExceptionMessage() << Endl;
            return 1;
        }

        try {
            bootstrap.Init();
            bootstrap.Start();
        } catch (...) {
            Cerr << "main bootstrap start: " << CurrentExceptionMessage()
                 << Endl;
            bootstrap.Stop();
            return 1;
        }

        int exitCode = AppMain(bootstrap.GetShouldContinue());

        try {
            bootstrap.Stop();
        } catch (...) {
            Cerr << "main bootstrap stop: " << CurrentExceptionMessage()
                 << Endl;
            return 1;
        }

        return exitCode;
    } catch (...) {
        Cerr << "main: " << CurrentExceptionMessage() << Endl;
        return 1;
    }
}

}   // namespace NCloud
