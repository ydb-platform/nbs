#include "keyring.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/hash_set.h>
#include <util/stream/file.h>
#include <util/string/split.h>
#include <util/string/strip.h>
#include <util/system/spin_wait.h>

#if defined(_linux_) && !defined(_bionic_)
#define HAVE_KEYRING
#endif

#if defined(HAVE_KEYRING)
#include <asm/unistd.h>
#include <linux/keyctl.h>
#endif

namespace NCloud {

#if defined(HAVE_KEYRING)

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString KeyringType = "keyring";
const TString UserKeyType = "user";
const ui32 InvalidCode = static_cast<ui32>(-1);
const ui32 MaxRetries = 10;

template <typename T>
ui32 RetriableExecute(T&& block, const TString& operation)
{
    TSpinWait sw;
    while (sw.C < MaxRetries) {
        ui32 res = block();
        if (res != InvalidCode) {
            return res;
        }

        if (errno != EAGAIN) {
            auto err = errno;
            auto msg = strerror(errno);
            Cerr << operation << " failed with error: "
                << err << ", " << msg << Endl;
            return res;
        }

        sw.Sleep();
    }

    Cerr << operation << " failed after reaching retry limit" << Endl;
    return InvalidCode;
}

#define RETRIABLE_SYSCALL(op, ...)                                             \
    RetriableExecute([&] { return syscall(op, __VA_ARGS__); }, #op);

////////////////////////////////////////////////////////////////////////////////

ui32 SysAddKey(
    const TString& kind,
    const TString& desc,
    const TString& payload,
    ui32 keyring)
{
    return RETRIABLE_SYSCALL(
        __NR_add_key,
        kind.c_str(),
        desc.c_str(),
        payload.c_str(),
        payload.size(),
        keyring);
}

ui32 SysKeyCtlSearch(ui32 keyring, const TString& kind, const TString& desc)
{
    return RETRIABLE_SYSCALL(
        __NR_keyctl,
        KEYCTL_SEARCH,
        keyring,
        kind.c_str(),
        desc.c_str(),
        0);
}

ui32 SysKeyCtlReadSize(ui32 key)
{
    return RETRIABLE_SYSCALL(__NR_keyctl, KEYCTL_READ, key, NULL, 0);
}

TString SysKeyCtlReadValue(ui32 key, ui32 size)
{
    TString res(size, 0);
    ui32 readSize = RETRIABLE_SYSCALL(
        __NR_keyctl,
        KEYCTL_READ,
        key,
        res.data(),
        size);

    if (readSize != size) {
        ythrow TServiceError(E_REJECTED)
            << "readSize (" << readSize << ")"
            << " should be equal size (" << size << ")";
    }
    return res;
}

TString SysKeyCtlDescribe(ui32 key)
{
    ui32 size = RETRIABLE_SYSCALL(__NR_keyctl, KEYCTL_DESCRIBE, key, NULL, 0);
    if (size == InvalidCode) {
        ythrow TServiceError(E_REJECTED) << "failed to describe keyring";
    }

    if (size == 0) {
        return {};
    }

    char buf[size];
    memset((void*)buf, 0, size);
    ui32 readSize = RETRIABLE_SYSCALL(__NR_keyctl, KEYCTL_DESCRIBE, key, buf, size);
    if (readSize != size) {
        ythrow TServiceError(E_REJECTED)
            << "readSize (" << readSize << ")"
            << " should be equal size (" << size << ")";
    }
    return buf;
}

bool SysKeyCtlSetPerm(ui32 key, ui32 mask)
{
    ui32 res = RETRIABLE_SYSCALL(__NR_keyctl, KEYCTL_SETPERM, key, mask);
    return res == 0;
}

bool SysKeyCtlLink(ui32 key, ui32 keyring)
{
    ui32 res = RETRIABLE_SYSCALL(__NR_keyctl, KEYCTL_LINK, key, keyring);
    return res == 0;
}

bool SysKeyCtlUnlink(ui32 key, ui32 keyring)
{
    ui32 res = RETRIABLE_SYSCALL(__NR_keyctl, KEYCTL_UNLINK, key, keyring);
    return res == 0;
}

ui32 SearchProcKeys(const TString& desc)
{
    static const TString KeysFilePath = "/proc/keys";
    static const ui32 KeySerialColumn = 0;
    static const ui32 KeyDescColumn = 8;

    TIFStream file(KeysFilePath);

    TString str;
    while (file.ReadLine(str)) {
        str = StripString(str);
        if (str.empty())
            continue;

        TVector<TStringBuf> splitted;
        StringSplitter(str).Split(' ').SkipEmpty().AddTo(&splitted);
        const auto& keyStr = splitted.at(KeySerialColumn);
        const auto& keyDesc = splitted.at(KeyDescColumn);

        if (keyDesc.size() == desc.size() + 1 &&
            keyDesc.back() == ':' &&
            keyDesc.StartsWith(desc))
        {
            return std::strtoul(keyStr.data(), nullptr, 16);
        }
    }
    return InvalidCode;
}

////////////////////////////////////////////////////////////////////////////////

ui32 GetRootKeySerial(TKeyring::ERootKeyring keyring)
{
    switch (keyring)
    {
        case TKeyring::Thread:
            return KEY_SPEC_THREAD_KEYRING;
        case TKeyring::Process:
            return KEY_SPEC_PROCESS_KEYRING;
        case TKeyring::Session:
            return KEY_SPEC_SESSION_KEYRING;
        case TKeyring::User:
            return KEY_SPEC_USER_KEYRING;
        case TKeyring::UserSession:
            return KEY_SPEC_USER_SESSION_KEYRING;
        default:
            Y_ABORT("Undefined root keyring: %d", keyring);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TKeyring::TKeyring(ui32 keySerial)
    : KeySerial(keySerial)
{}

TKeyring::TKeyring()
    : KeySerial(InvalidCode)
{}

bool TKeyring::operator==(const TKeyring& other) const
{
    return KeySerial == other.KeySerial;
}

TKeyring::operator bool() const
{
    return KeySerial != InvalidCode;
}

TKeyring TKeyring::Create(ui32 keySerial)
{
    auto size = SysKeyCtlReadSize(keySerial);
    if (size == InvalidCode) {
        return {};
    }

    return TKeyring(keySerial);
}

TKeyring TKeyring::GetRoot(ERootKeyring keyring)
{
    auto keySerial = GetRootKeySerial(keyring);
    return TKeyring(keySerial);
}

TKeyring TKeyring::GetProcKey(const TString& desc)
{
    auto keySerial = SearchProcKeys(desc);
    return TKeyring(keySerial);
}

ui32 TKeyring::GetId() const
{
    return KeySerial;
}

TString TKeyring::GetType() const
{
    auto desc = SysKeyCtlDescribe(KeySerial);
    auto n = desc.find_first_of(';');
    return desc.substr(0, n);
}

TString TKeyring::GetDesc() const
{
    auto desc = SysKeyCtlDescribe(KeySerial);
    auto n = desc.find_last_of(';');
    return desc.substr(n + 1);
}

ui32 TKeyring::GetValueSize() const
{
    return SysKeyCtlReadSize(KeySerial);
}

TString TKeyring::GetValue() const
{
    auto size = SysKeyCtlReadSize(KeySerial);
    if (size == InvalidCode) {
        return {};
    }

    return SysKeyCtlReadValue(KeySerial, size);
}

bool TKeyring::SetPerm(ui32 mask)
{
    return SysKeyCtlSetPerm(KeySerial, mask);
}

TKeyring TKeyring::AddKeyring(const TString& desc)
{
    auto key = SysAddKey(KeyringType, desc, {}, KeySerial);
    return TKeyring(key);
}

TKeyring TKeyring::SearchKeyring(const TString& desc) const
{
    auto key = SysKeyCtlSearch(KeySerial, KeyringType, desc);
    return TKeyring(key);
}

bool TKeyring::LinkKeyring(const TKeyring& keyring)
{
    return SysKeyCtlLink(keyring.KeySerial, KeySerial);
}

bool TKeyring::UnlinkKeyring(const TKeyring& keyring)
{
    return SysKeyCtlUnlink(keyring.KeySerial, KeySerial);
}

TKeyring TKeyring::AddUserKey(const TString& key, const TString& value)
{
    auto userKey = SysAddKey(UserKeyType, key, value, KeySerial);
    return TKeyring(userKey);
}

TVector<TKeyring> TKeyring::GetUserKeys() const
{
    auto size = SysKeyCtlReadSize(KeySerial);
    if (size == InvalidCode) {
        return {};
    }

    auto data = SysKeyCtlReadValue(KeySerial, size);

    static_assert(sizeof(TKeyring) == sizeof(ui32), "TKeyring size must match ui32");
    auto count = data.size() / sizeof(ui32);
    TVector<TKeyring> keyrings;
    keyrings.resize(count);
    std::memcpy(keyrings.data(), data.data(), count * sizeof(ui32));

    return keyrings;
}

void PrintKey(TKeyring key, ui32 level, THashSet<TKeyring>& allKeys)
{
    for (ui32 i = 0; i < level; ++i) {
        Cerr << "  ";
    }
    Cerr << "- " << key.GetDesc();

    if (allKeys.contains(key)) {
        Cerr << " loop" << Endl;
        return;
    }

    allKeys.insert(key);
    Cerr << Endl;

    if (key.GetType() == UserKeyType) {
        return;
    }

    auto userKeys = key.GetUserKeys();
    for (auto userKey: userKeys) {
        PrintKey(userKey, level + 1, allKeys);
    }
}

void PrintAllKeys()
{
    auto process = TKeyring::GetRoot(TKeyring::Process);
    auto tempKey = process.AddKeyring("nbs-temp-key");

    THashSet<TKeyring> allKeys;
    PrintKey(process, 0, allKeys);
    Cerr << "all keys count: " << allKeys.size() << Endl;

    process.UnlinkKeyring(tempKey);
}

#else

TKeyring::TKeyring()
{
    Y_ABORT("Not implemented");
}

TKeyring TKeyring::Create(ui32 keySerial)
{
    Y_UNUSED(keySerial);
    Y_ABORT("Not implemented");
}

TKeyring TKeyring::GetRoot(ERootKeyring keyring)
{
    Y_UNUSED(keyring);
    Y_ABORT("Not implemented");
}

TKeyring TKeyring::GetProcKey(const TString& desc)
{
    Y_UNUSED(desc);
    Y_ABORT("Not implemented");
}

TKeyring& TKeyring::operator=(const TKeyring& other)
{
    Y_UNUSED(other);
    Y_ABORT("Not implemented");
}

bool TKeyring::operator==(const TKeyring& other) const
{
    Y_UNUSED(other);
    Y_ABORT("Not implemented");
}

TKeyring::operator bool() const
{
    Y_ABORT("Not implemented");
}

ui32 TKeyring::GetId() const
{
    Y_ABORT("Not implemented");
}

TString TKeyring::GetType() const
{
    Y_ABORT("Not implemented");
}

TString TKeyring::GetDesc() const
{
    Y_ABORT("Not implemented");
}

ui32 TKeyring::GetValueSize() const
{
    Y_ABORT("Not implemented");
}

TString TKeyring::GetValue() const
{
    Y_ABORT("Not implemented");
}

bool TKeyring::SetPerm(ui32 mask)
{
    Y_UNUSED(mask);
    Y_ABORT("Not implemented");
}

TKeyring TKeyring::AddKeyring(const TString& desc)
{
    Y_UNUSED(desc);
    Y_ABORT("Not implemented");
}

TKeyring TKeyring::SearchKeyring(const TString& desc) const
{
    Y_UNUSED(desc);
    Y_ABORT("Not implemented");
}

bool TKeyring::LinkKeyring(const TKeyring& keyring)
{
    Y_UNUSED(keyring);
    Y_ABORT("Not implemented");
}

bool TKeyring::UnlinkKeyring(const TKeyring& keyring)
{
    Y_UNUSED(keyring);
    Y_ABORT("Not implemented");
}

TKeyring TKeyring::AddUserKey(const TString& desc, const TString& value)
{
    Y_UNUSED(desc);
    Y_UNUSED(value);
    Y_ABORT("Not implemented");
}

TVector<TKeyring> TKeyring::GetUserKeys() const
{
    Y_ABORT("Not implemented");
}

void PrintAllKeys()
{
    Y_ABORT("Not implemented");
}

#endif

}   // namespace NCloud
