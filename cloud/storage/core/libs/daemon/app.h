#pragma once

#include "public.h"

#include <util/generic/yexception.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

int AppMain(TProgramShouldContinue& shouldContinue);
void AppStop(int exitCode);

void ConfigureSignals();

template <typename TBootstrap>
int DoMain(TBootstrap& bootstrap, int argc, char** argv)
{
    try {
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
            Cerr << "main bootstrap start: " << CurrentExceptionMessage() << Endl;
            bootstrap.Stop();
            return 1;
        }

        int exitCode = AppMain(bootstrap.GetShouldContinue());

        try {
            bootstrap.Stop();
        } catch (...) {
            Cerr << "main bootstrap stop: " << CurrentExceptionMessage() << Endl;
            return 1;
        }

        return exitCode;
    } catch (...) {
        Cerr << "main: " << CurrentExceptionMessage() << Endl;
        return 1;
    }
}

}   // namespace NCloud
