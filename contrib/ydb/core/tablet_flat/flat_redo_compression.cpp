#include "flat_redo_compression.h"
#include "flat_sausage_slicer.h"

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NTabletFlatExecutor {
namespace {

class TRedoCompressionActor : public TActorBootstrapped<TRedoCompressionActor> {
public:
    TRedoCompressionActor(TActorId owner, ui32 step, TString redo)
        : Owner(owner)
        , Step(step)
        , Redo(std::move(redo))
    {}

    void Bootstrap() {
        Send(Owner, new TEvRedoEncoded(Step, NPageCollection::TSlicer::Lz4()->Encode(Redo)));
        PassAway();
    }

private:
    const TActorId Owner;
    const ui32 Step;
    TString Redo;
};

}

IActor* CreateRedoCompressionActor(TActorId owner, ui32 step, TString redo) {
    return new TRedoCompressionActor(owner, step, std::move(redo));
}

}
