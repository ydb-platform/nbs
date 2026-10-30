/*******************************************************************************

Local subscriptions to Blockstore configuration publications.
Each subscriber has one subscription to the entire configuration. Registration
and removal responses go to the requester; further change events go to
Subscriber or the requester when Subscriber is empty. Registration sends a
change notification immediately. Consumers read the current snapshot from the
provider and send no acknowledgement. Unsubscribe before stopping the consumer
actor.

*******************************************************************************/

#pragma once

#include <cloud/blockstore/libs/kikimr/components.h>

#include <contrib/ydb/library/actors/core/actorid.h>
#include <contrib/ydb/library/actors/core/event_local.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// Local ConfigsManager messages for registration, removal, and change delivery.
// Callers send requests to MakeConfigsManagerServiceId().
struct TEvConfigsManager
{
    enum EEvents
    {
        EvBegin = TBlockStoreEvents::CONFIGS_MANAGER_START,
        EvSetConfigSubscriptionRequest = EvBegin + 1,
        EvSetConfigSubscriptionResponse,
        EvRemoveConfigSubscriptionRequest,
        EvRemoveConfigSubscriptionResponse,
        EvConfigChanged,
        EvEnd
    };

    static_assert(EvEnd < TBlockStoreEvents::CONFIGS_MANAGER_END);

    // Subscription request; repeated registration refreshes the initial notice.
    struct TEvSetConfigSubscriptionRequest final
        : NActors::TEventLocal<
              TEvSetConfigSubscriptionRequest,
              EvSetConfigSubscriptionRequest>
    {
        // Notification recipient; empty selects the request sender.
        const NActors::TActorId Subscriber;

        explicit TEvSetConfigSubscriptionRequest(
            NActors::TActorId subscriber = {});
    };

    // Registration response to the requester, sent before the initial notice.
    struct TEvSetConfigSubscriptionResponse final
        : NActors::TEventLocal<
              TEvSetConfigSubscriptionResponse,
              EvSetConfigSubscriptionResponse>
    {
    };

    // Removal request; consumers send it before stopping their actor.
    struct TEvRemoveConfigSubscriptionRequest final
        : NActors::TEventLocal<
              TEvRemoveConfigSubscriptionRequest,
              EvRemoveConfigSubscriptionRequest>
    {
        // Subscription owner to remove; empty selects the request sender.
        const NActors::TActorId Subscriber;

        explicit TEvRemoveConfigSubscriptionRequest(
            NActors::TActorId subscriber = {});
    };

    // Removal response, including when the subscriber was already absent.
    struct TEvRemoveConfigSubscriptionResponse final
        : NActors::TEventLocal<
              TEvRemoveConfigSubscriptionResponse,
              EvRemoveConfigSubscriptionResponse>
    {
    };

    // Publication notice; read the current provider snapshot without replying.
    // The provider may already contain a newer publication when this is
    // handled.
    struct TEvConfigChanged final
        : NActors::TEventLocal<TEvConfigChanged, EvConfigChanged>
    {
    };
};

inline TEvConfigsManager::TEvSetConfigSubscriptionRequest::
    TEvSetConfigSubscriptionRequest(NActors::TActorId subscriber)
    : Subscriber(subscriber)
{}

inline TEvConfigsManager::TEvRemoveConfigSubscriptionRequest::
    TEvRemoveConfigSubscriptionRequest(NActors::TActorId subscriber)
    : Subscriber(subscriber)
{}

}   // namespace NCloud::NBlockStore
