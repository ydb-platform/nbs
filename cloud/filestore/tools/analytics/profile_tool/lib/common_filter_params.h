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

class TCommonFilterParams
{
public:
    explicit TCommonFilterParams(NLastGetopt::TOpts& opts);

    TMaybe<TString> GetFileSystemId(
        const NLastGetopt::TOptsParseResultException& parseResult) const;
    TMaybe<ui64> GetNodeId(
        const NLastGetopt::TOptsParseResultException& parseResult) const;
    TMaybe<ui64> GetHandle(
        const NLastGetopt::TOptsParseResultException& parseResult) const;
    TMaybe<TInstant> GetSince(
        const NLastGetopt::TOptsParseResultException& parseResult) const;
    TMaybe<TInstant> GetUntil(
        const NLastGetopt::TOptsParseResultException& parseResult) const;
};

}   // namespace NCloud::NFileStore::NProfileTool
