#include "filesystem_event.h"

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/system/mutex.h>

#include <algorithm>
#include <atomic>

namespace NCloud::NFileStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TMultiFileSystemEventHandler final
    : public IMultiFileSystemEventHandler
{
    using THandlers = TVector<IFileSystemEventHandlerPtr>;

private:
    TMutex Lock;
    THashMap<TString, THandlers> Handlers;
    // Read without the lock, see HasConsumers.
    std::atomic<ui64> HandlerCount = 0;

public:
    void Register(
        const TString& fileSystemId,
        IFileSystemEventHandlerPtr handler) override
    {
        with_lock (Lock) {
            Handlers[fileSystemId].push_back(std::move(handler));
            ++HandlerCount;
        }
    }

    void Unregister(
        const TString& fileSystemId,
        const IFileSystemEventHandlerPtr& handler) override
    {
        with_lock (Lock) {
            auto it = Handlers.find(fileSystemId);
            if (it == Handlers.end()) {
                return;
            }

            auto& handlers = it->second;
            const auto removed = std::remove(
                handlers.begin(),
                handlers.end(),
                handler);
            HandlerCount -= std::distance(removed, handlers.end());
            handlers.erase(removed, handlers.end());

            if (handlers.empty()) {
                Handlers.erase(it);
            }
        }
    }

    void OnEvent(const NProto::TFileSystemEvent& event) override
    {
        //
        // Handlers are called outside the lock so that they are free to
        // register or unregister handlers.
        //

        THandlers handlers;
        with_lock (Lock) {
            if (const auto* h = Handlers.FindPtr(event.GetFileSystemId())) {
                handlers = *h;
            }
        }

        for (const auto& handler: handlers) {
            handler->OnEvent(event);
        }
    }

    bool HasConsumers() const override
    {
        return HandlerCount.load(std::memory_order_relaxed) != 0;
    }

    void OnDisconnect(ui64 sourceId) override
    {
        THandlers handlers;
        with_lock (Lock) {
            for (const auto& [_, h]: Handlers) {
                handlers.insert(handlers.end(), h.begin(), h.end());
            }
        }

        for (const auto& handler: handlers) {
            handler->OnDisconnect(sourceId);
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IMultiFileSystemEventHandlerPtr CreateMultiFileSystemEventHandler()
{
    return std::make_shared<TMultiFileSystemEventHandler>();
}

}   // namespace NCloud::NFileStore
