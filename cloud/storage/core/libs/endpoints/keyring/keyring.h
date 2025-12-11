#pragma once

#include <cloud/storage/core/libs/endpoints/iface/public.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

class TKeyring
{
private:
    ui32 KeySerial;

    TKeyring(ui32 keySerial);

public:
    TKeyring();
    TKeyring(const TKeyring& other) = default;

    static constexpr ui32 PosAllPerm = 0x3f000000;
    static constexpr ui32 UsrAllPerm = 0x003f0000;
    static constexpr ui32 GrpAllPerm = 0x00003f00;
    static constexpr ui32 OthAllPerm = 0x0000003f;

    enum ERootKeyring
    {
        Thread = 0,    // thread-specific keyring
        Process,       // process-specific keyring
        Session,       // session-specific keyring
        User,          // UID-specific keyring
        UserSession,   // UID-session keyring
    };

    static TKeyring Create(ui32 keySerial);
    static TKeyring GetRoot(ERootKeyring keyring);
    static TKeyring GetProcKey(const TString& desc);

    TKeyring& operator=(const TKeyring& other) = default;
    bool operator==(const TKeyring& other) const;

    operator bool() const;

    ui32 GetId() const;
    TString GetType() const;
    TString GetDesc() const;
    ui32 GetValueSize() const;
    TString GetValue() const;

    bool SetPerm(ui32 mask);

    TKeyring AddKeyring(const TString& desc);
    TKeyring SearchKeyring(const TString& desc) const;

    bool LinkKeyring(const TKeyring& keyring);
    bool UnlinkKeyring(const TKeyring& keyring);

    TKeyring AddUserKey(const TString& desc, const TString& value);
    TVector<TKeyring> GetUserKeys() const;
};

////////////////////////////////////////////////////////////////////////////////

void PrintAllKeys();

}   // namespace NCloud
