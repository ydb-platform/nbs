#pragma once

#include "public.h"

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/file.h>
#include <util/system/fstat.h>

namespace NCloud::NFileStore {
namespace NLowLevel {

////////////////////////////////////////////////////////////////////////////////

TFile Open(const TString& path, int flags, int mode);
TFile Open(const TFile& handle, int flags, int mode);
TFile OpenAt(const TFile& handle, const TString& name, int flags, int mode);

void MkDirAt(const TFile& handle, const TString& name, int mode);
void MkSockAt(const TFile& handle, const TString& name, int mode);

void RenameAt(
    const TFile& handle,
    const TString& name,
    const TFile& newhandle,
    const TString& newname,
    unsigned int flags);

void LinkAt(const TFile& node, const TFile& parent, const TString& name);
void SymLinkAt(const TString target, const TFile& handle, const TString& name);
void UnlinkAt(const TFile& handle, const TString& name, bool directory);

TString ReadLink(const TFile& handle);

TFileStat Stat(const TFile& handle);
TFileStat StatAt(const TFile& handle, const TString& name);

TVector<std::pair<TString, TFileStat>> ListDirAt(const TFile& handle, bool ignoreErrors);

//
// Attrs
//

void Access(const TFile& handle, int mode);
void Chmod(const TFile& handle, int mode);
void Chown(const TFile& handle, int uid, int gid);
void Utimes(const TFile& handle, TInstant atime, TInstant mtime);
void Truncate(const TFile& handle, size_t size);
void Allocate(const TFile& handle, int flags, off_t offset, off_t length);

//
// X Attrs
//

TString GetXAttr(const TFile& handle, const TString& name);
TVector<TString> ListXAttrs(const TFile& handle);
void RemoveXAttr(const TFile& handle, const TString& name);
void SetXAttr(const TFile& handle, const TString& name, const TString& value);

//
// Locks
//

bool AcquireLock(const TFile& handle, int offset, int len, bool shared);
bool TestLock(const TFile& handle, int offset, int len, bool shared);
void ReleaseLock(const TFile& handle, int offset, int len);
bool Flock(const TFile& handle, int operation);

}   // namespace NLowLevel
}   // namespace NCloud::NFileStore
