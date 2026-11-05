#include "filesystem_event.h"

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/system/mutex.h>

#include <algorithm>

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

public:
    void Register(
        const TString& fileSystemId,
        IFileSystemEventHandlerPtr handler) override
    {
        with_lock (Lock) {
            Handlers[fileSystemId].push_back(std::move(handler));
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
            handlers.erase(
                std::remove(handlers.begin(), handlers.end(), handler),
                handlers.end());

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

    void OnDisconnect(ui64 tabletId) override
    {
        THandlers handlers;
        with_lock (Lock) {
            for (const auto& [_, h]: Handlers) {
                handlers.insert(handlers.end(), h.begin(), h.end());
            }
        }

        for (const auto& handler: handlers) {
            handler->OnDisconnect(tabletId);
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
