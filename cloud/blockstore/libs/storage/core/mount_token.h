#pragma once

#include "public.h"

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TMountToken
{
    enum class EFormat
    {
        EMPTY,
        SHA384_V1,
    };

    enum class EStatus
    {
        OK,
        FORMAT_UNKNOWN,
        TOKEN_CORRUPTED,
    };

    EFormat Format;
    TString Salt;
    TString Hash;

    TMountToken()
        : Format(EFormat::EMPTY)
    {}

    TMountToken(EFormat format, const TString& salt, const TString& hash)
        : Format(format)
        , Salt(salt)
        , Hash(hash)
    {}

    void SetSecret(EFormat format, const TString& secret);
    void SetSecret(EFormat format, const TString& secret, const TString& salt);
    bool VerifySecret(const TString& secret) const;

    TString ToString() const;
    EStatus ParseString(const TString& token);
};

}   // namespace NCloud::NBlockStore::NStorage
