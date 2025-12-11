#include "components.h"

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TComponent
{
    int Component;
    TString Name;
};

#define FILESTORE_DECLARE_COMPONENT(component) \
    {TFileStoreComponents::component,          \
     "NFS_" #component},   // FILESTORE_DECLARE_COMPONENT

static const TComponent Components[] = {
    FILESTORE_COMPONENTS(FILESTORE_DECLARE_COMPONENT)};

#undef FILESTORE_DECLARE_COMPONENT

}   // namespace

////////////////////////////////////////////////////////////////////////////////

const TString& GetComponentName(int component)
{
    if (component > TFileStoreComponents::START &&
        component < TFileStoreComponents::END)
    {
        return Components[component - TFileStoreComponents::START - 1].Name;
    }

    static const TString UNKNOWN = "UNKNOWN";
    return UNKNOWN;
}

}   // namespace NCloud::NFileStore::NStorage
