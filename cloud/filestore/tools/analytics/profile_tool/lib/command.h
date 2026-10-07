#pragma once

#include "time_range.h"

#include <library/cpp/getopt/small/last_getopt.h>

class IEventProcessor;

namespace NCloud::NFileStore::NProfileTool {

void PrintProfileLogProgress(const TProfileLogFile& file);

////////////////////////////////////////////////////////////////////////////////

class TCommand
{
private:
    bool IgnoreErrors = false;

protected:
    NLastGetopt::TOpts Opts;
    TMaybe<NLastGetopt::TOptsParseResultException> OptsParseResult;

    TVector<TProfileLogFile> ProfileLogFiles;

public:
    TCommand();

    virtual ~TCommand() = default;

    int Run(int argc, const char** argv);

    const NLastGetopt::TOpts& GetOpts() const;

protected:
    int ProcessProfileLogs(IEventProcessor& processor);

    static int ProcessProfileLog(
        const TString& path,
        IEventProcessor& processor,
        bool ignoreErrors);

    virtual bool Init(NLastGetopt::TOptsParseResultException& parseResult);
    virtual int Execute() = 0;
};

}   // namespace NCloud::NFileStore::NProfileTool
