#pragma once

#include <util/datetime/base.h>
#include <util/generic/maybe.h>

namespace NLastGetopt {

////////////////////////////////////////////////////////////////////////////////

class TOpts;
class TOptsParseResultException;

}   // namespace NLastGetopt

namespace NCloud::NFileStore::NProfileTool {

////////////////////////////////////////////////////////////////////////////////

struct TTimeRange
{
    TMaybe<TInstant> Since;
    TMaybe<TInstant> Until;
};

class TCommonFilterParams
{
private:
    const TInstant ReferenceTime;

public:
    explicit TCommonFilterParams(
        NLastGetopt::TOpts& opts,
        TInstant referenceTime = TInstant::Now());

    TMaybe<TString> GetFileSystemId(
        const NLastGetopt::TOptsParseResultException& parseResult) const;
    TMaybe<ui64> GetNodeId(
        const NLastGetopt::TOptsParseResultException& parseResult) const;
    TMaybe<ui64> GetHandle(
        const NLastGetopt::TOptsParseResultException& parseResult) const;
    TTimeRange GetTimeRange(
        const NLastGetopt::TOptsParseResultException& parseResult) const;
};

}   // namespace NCloud::NFileStore::NProfileTool
