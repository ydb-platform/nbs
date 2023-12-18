#pragma once

#include <library/cpp/getopt/small/last_getopt.h>

namespace NCloud::NFileStore::NProfileTool {

////////////////////////////////////////////////////////////////////////////////

class TCommand
{
protected:
    NLastGetopt::TOpts Opts;
    TMaybe<NLastGetopt::TOptsParseResultException> OptsParseResult;

    TString PathToProfileLog;

public:
    TCommand();

    virtual ~TCommand() = default;

    int Run(int argc, const char** argv);

    const NLastGetopt::TOpts& GetOpts() const;

protected:
    virtual bool Init(NLastGetopt::TOptsParseResultException& parseResult);
    virtual int Execute() = 0;
};

}   // namespace NCloud::NFileStore::NProfileTool
