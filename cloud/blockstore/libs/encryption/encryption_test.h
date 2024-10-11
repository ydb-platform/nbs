#pragma once

#include "public.h"

#include <util/system/file.h>
#include <util/system/fs.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

class TEncryptionKeyFile
{
private:
    TFile File;

public:
    TEncryptionKeyFile(
            const TString& key,
            const TString& filePath = "test_key_file")
        : File(filePath, EOpenModeFlag::CreateAlways | EOpenModeFlag::RdWr)
    {
        File.Write(key.data(), key.size());
        File.Flush();
        File.Close();
    }

    ~TEncryptionKeyFile()
    {
        NFs::Remove(File.GetName());
    }

    const TString& GetPath()
    {
        return File.GetName();
    }
};

}   // namespace NCloud::NBlockStore
