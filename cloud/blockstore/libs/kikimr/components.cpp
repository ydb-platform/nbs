#include "components.h"

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TComponent
{
    int Component;
    TString Name;
};

#define BLOCKSTORE_DECLARE_COMPONENT(component) \
    {TBlockStoreComponents::component,          \
     "BLOCKSTORE_" #component},   // BLOCKSTORE_DECLARE_COMPONENT

static const TComponent Components[] = {
    BLOCKSTORE_COMPONENTS(BLOCKSTORE_DECLARE_COMPONENT)};

#undef BLOCKSTORE_DECLARE_COMPONENT

}   // namespace

////////////////////////////////////////////////////////////////////////////////

const TString& GetComponentName(int component)
{
    if (component > TBlockStoreComponents::START &&
        component < TBlockStoreComponents::END)
    {
        return Components[component - TBlockStoreComponents::START - 1].Name;
    }

    static const TString UNKNOWN = "UNKNOWN";
    return UNKNOWN;
}

}   // namespace NCloud::NBlockStore
