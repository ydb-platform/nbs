#include "node_cache.h"

#include <cloud/filestore/libs/service/request.h>

#include <util/generic/vector.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

TNode* TNodeCache::AddNode(const NProto::TNodeAttr& attrs)
{
    auto [it, inserted] = Id2Node.emplace(attrs.GetId(), TNode(attrs));
    Y_ABORT_UNLESS(inserted, "failed to insert %s",
        DumpMessage(attrs).data());

    return &it->second;
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
    auto it = Id2Node.find(ino);
    if (it == Id2Node.end()) {
        // we lose our cache after restart, so we should expect forget requests
        // targeting nodes that are absent from our cache
        // see NBS-2102

        return;
    }

    count = it->second.UnRef(count);
    if (count == 0) {
        // do not pass element itself
        Id2Node.erase(it);
    }
}

TNode* TNodeCache::FindNode(ui64 ino)
{
    return Id2Node.FindPtr(ino);
}

////////////////////////////////////////////////////////////////////////////////

void TXAttrCache::Add(
    ui64 ino,
    const TString& name,
    const TString& value,
    ui64 version)
{
    const auto* current = Get(ino, name);
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
