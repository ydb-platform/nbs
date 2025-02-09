#include "node_cache.h"

#include <cloud/filestore/libs/service/request.h>

#include <util/generic/vector.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

TNode* TNodeCache::AddNode(const NProto::TNodeAttr& attrs)
{
    auto [it, inserted] = Nodes.emplace(attrs);
    Y_ABORT_UNLESS(inserted, "failed to insert %s",
        DumpMessage(attrs).data());

    auto* node = (TNode*)&*it;
    return node;
}

TNode* TNodeCache::TryAddNode(const NProto::TNodeAttr& attrs)
{
    auto* node = FindNode(attrs.GetId());
    if (!node) {
        node = AddNode(attrs);
    } else {
        node->UpdateAttrs(attrs);
        node->Ref();
    }

    return node;
}

void TNodeCache::ForgetNode(ui64 ino, size_t count)
{
    auto it = Nodes.find(ino);
    if (it == Nodes.end()) {
        // we lose our cache after restart, so we should expect forget requests
        // targeting nodes that are absent from our cache
        // see NBS-2102

        return;
    }

    count = const_cast<TNode&>(*it).UnRef(count);
    if (count == 0) {
        // do not pass element itself
        Nodes.erase(it);
    }
}

TNode* TNodeCache::FindNode(ui64 ino)
{
    auto it = Nodes.find(ino);
    return it != Nodes.end() ? (TNode*)&*it : nullptr;
}

////////////////////////////////////////////////////////////////////////////////

void TXAttrCache::Add(
    ui64 ino,
    const TString& name,
    const TString& value,
    ui64 version)
{
    auto current = Get(ino, name);
    if (!current || current->Version < version) {
        Cache.Update(
            TKey{ino, name},
            TXAttr{
                .Name = name,
                .Value = value,
                .Version = version,
                .UpdateTime = Timer->Now()
            }
        );
    }
}

void TXAttrCache::AddAbsent(ui64 ino, const TString& name)
{
    Cache.Update(
        TKey{ino, name},
        TXAttr{
            .Name = name,
            .Value = Nothing(),
            .Version = 0,
            .UpdateTime = Timer->Now()
        }
    );
}

const TXAttr* TXAttrCache::Get(ui64 ino, const TString& name)
{
    auto it = Cache.Find({ino, name});
    if (it != Cache.End() && (Timer->Now() - it.Value().UpdateTime) < Timeout) {
        return &it.Value();
    }

    return nullptr;
}

void TXAttrCache::Forget(ui64 ino, const TString& name)
{
    auto it = Cache.Find({ino, name});
    if (it != Cache.End()) {
        Cache.Erase(it);
    }
}

}   // namespace NCloud::NFileStore::NFuse
