#include "node_cache.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/service/request.h>

#include <util/generic/vector.h>
#include <util/string/builder.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

bool TNodeCacheShard::UpdateNode(
    const NProto::TNodeAttr& attrs,
    ui64 version)
{
    auto g = Guard(Lock);

    auto [it, inserted] = Id2Node.emplace(attrs.GetId(), TNode(attrs));
    auto& node = it->second;
    bool updated = false;
    if (inserted) {
        node.LastUpdateVersion = version;
        updated = true;
    } else {
        if (version >= node.LastUpdateVersion) {
            node.UpdateAttrs(attrs, version);

            if (!node.IsValid()) {
                ReportNodeCacheInvalidNode(TStringBuilder()
                    << "fs: " << FileSystemId
                    << ", attrs: " << attrs.ShortUtf8DebugString()
                    << ", version: " << version);
            }

            updated = true;
        }

        node.Ref();
    }

    return updated;
}

void TNodeCacheShard::InvalidateNode(ui64 ino, ui64 version)
{
    auto g = Guard(Lock);

    NProto::TNodeAttr attrs;
    attrs.SetId(ino);
    auto [it, inserted] = Id2Node.emplace(ino, TNode(attrs));
    if (version >= it->second.LastUpdateVersion) {
        if (!inserted) {
            it->second.Attrs = std::move(attrs);
        }

        it->second.LastUpdateVersion = version;
    }
}

void TNodeCacheShard::ForgetNode(ui64 ino, size_t count)
{
    auto g = Guard(Lock);

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

ui64 TNodeCacheShard::GetNodeVersion(ui64 ino) const
{
    auto g = Guard(Lock);

    const auto* node = Id2Node.FindPtr(ino);
    return node ? node->LastUpdateVersion : 0;
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
