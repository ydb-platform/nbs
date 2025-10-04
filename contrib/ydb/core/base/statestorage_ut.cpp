#include "statestorage.h"
#include "tabletid.h"

#include <util/generic/xrange.h>
#include <library/cpp/testing/unittest/registar.h>
#include <unordered_set>

namespace NStateStorageOld {

static const ui32 Primes[128] = {
    104743, 105023, 105359, 105613,
    104759, 105031, 105361, 105619,
    104761, 105037, 105367, 105649,
    104773, 105071, 105373, 105653,
    104779, 105097, 105379, 105667,
    104789, 105107, 105389, 105673,
    104801, 105137, 105397, 105683,
    104803, 105143, 105401, 105691,
    104827, 105167, 105407, 105701,
    104831, 105173, 105437, 105727,
    104849, 105199, 105449, 105733,
    104851, 105211, 105467, 105751,
    104869, 105227, 105491, 105761,
    104879, 105229, 105499, 105767,
    104891, 105239, 105503, 105769,
    104911, 105251, 105509, 105817,
    104917, 105253, 105517, 105829,
    104933, 105263, 105527, 105863,
    104947, 105269, 105529, 105871,
    104953, 105277, 105533, 105883,
    104959, 105319, 105541, 105899,
    104971, 105323, 105557, 105907,
    104987, 105331, 105563, 105913,
    104999, 105337, 105601, 105929,
    105019, 105341, 105607, 105943,
    105953, 106261, 106487, 106753,
    105967, 106273, 106501, 106759,
    105971, 106277, 106531, 106781,
    105977, 106279, 106537, 106783,
    105983, 106291, 106541, 106787,
    105997, 106297, 106543, 106801,
    106013, 106303, 106591, 106823,
};

class TStateStorageRingWalker {
    const ui32 Sz;
    const ui32 Delta;
    ui32 A;
public:
    TStateStorageRingWalker(ui32 hash, ui32 sz)
        : Sz(sz)
        , Delta(Primes[hash % 128])
        , A(hash + Delta)
    {
        Y_DEBUG_ABORT_UNLESS(Delta > Sz);
    }

    ui32 Next() {
        A += Delta;
        return (A % Sz);
    }
};

} // namespace NStateStorageOld

namespace NKikimr {

Y_UNIT_TEST_SUITE(TStateStorageConfig) {

    void FillStateStorageInfo(TStateStorageInfo *info, ui32 replicas, ui32 nToSelect, ui32 replicasInRing, bool useRingSpecificNodeSelection) {
        info->NToSelect = nToSelect;

        info->Rings.resize(replicas);
        for (ui32 i : xrange(replicas)) {
            for (ui32 j : xrange(replicasInRing)) {
                info->Rings[i].Replicas.push_back(TActorId(i, i, i + j, i));
                info->Rings[i].UseRingSpecificNodeSelection = useRingSpecificNodeSelection;
            }
        }
    }

    ui64 StabilityRun(ui32 replicas, ui32 nToSelect, ui32 replicasInRing, bool useRingSpecificNodeSelection) {
        ui64 retHash = 0;

        TStateStorageInfo info;
        FillStateStorageInfo(&info, replicas, nToSelect, replicasInRing, useRingSpecificNodeSelection);

        TStateStorageInfo::TSelection selection;
        for (ui64 tabletId = 8000000; tabletId < 9000000; ++tabletId) {
            info.SelectReplicas(tabletId, &selection);
            std::unordered_set<TActorId> ids;
            for (ui32 i : xrange(selection.Sz)) {
                ids.insert(selection.SelectedReplicas[i]);
            }
            Y_ABORT_UNLESS(ids.size() == selection.Sz);
            Y_ABORT_UNLESS(nToSelect == selection.Sz);
            for (ui32 idx : xrange(nToSelect))
                retHash = CombineHashes<ui64>(retHash, selection.SelectedReplicas[idx].Hash());
        }
        return retHash;
    }

    double UniqueCombinationsRun(ui32 replicas, ui32 nToSelect, ui32 replicasInRing, bool useRingSpecificNodeSelection) {
        const ui64 tabletStartId = 8000000;
        const ui64 tabletCount = 1000000;
        TStateStorageInfo info;
        FillStateStorageInfo(&info, replicas, nToSelect, replicasInRing, useRingSpecificNodeSelection);

        THashSet<ui64> hashes;

        TStateStorageInfo::TSelection selection;
        for (ui64 tabletId = tabletStartId; tabletId < tabletStartId + tabletCount; ++tabletId) {
            ui64 selectionHash = 0;
            info.SelectReplicas(tabletId, &selection);
            Y_ABORT_UNLESS(nToSelect == selection.Sz);
            for (ui32 idx : xrange(nToSelect))
                selectionHash = CombineHashes<ui64>(selectionHash, selection.SelectedReplicas[idx].Hash());
            hashes.insert(selectionHash);
        }
        return static_cast<double>(hashes.size()) / static_cast<double>(tabletCount);
    }

    Y_UNIT_TEST(TestReplicaSelection) {
        UNIT_ASSERT(StabilityRun(3, 3, 1, false) == 17606246762804570019ULL);
        UNIT_ASSERT(StabilityRun(13, 3, 1, false) == 6799095354188407094ULL);
        UNIT_ASSERT(StabilityRun(13, 9, 1, false) == 9959984117877048199ULL);
        UNIT_ASSERT(StabilityRun(3, 3, 1, true) == 17606246762804570019ULL);
        UNIT_ASSERT(StabilityRun(13, 3, 1, true) == 6799095354188407094ULL);
        UNIT_ASSERT(StabilityRun(13, 9, 1, true) == 9959984117877048199ULL);
    }

    Y_UNIT_TEST(TestMultiReplicaFailDomains) {
        UNIT_ASSERT(StabilityRun(3, 3, 3, false) == 12043409773822600429ULL);
        UNIT_ASSERT(StabilityRun(13, 3, 5, false) == 16389704234708466102ULL);
        UNIT_ASSERT(StabilityRun(13, 9, 8, false) == 15827315848675537518ULL);
        UNIT_ASSERT(StabilityRun(3, 3, 3, true) == 7845257406715748850ULL);
        UNIT_ASSERT(StabilityRun(13, 3, 5, true) == 16411438521907095913ULL);
        UNIT_ASSERT(StabilityRun(13, 9, 8, true) == 5026957911653120252ULL);
    }

    Y_UNIT_TEST(TestReplicaSelectionUniqueCombinations) {
        UNIT_ASSERT_DOUBLES_EQUAL(UniqueCombinationsRun(13, 3, 1, false), 0.000205, 1e-7);
        UNIT_ASSERT_DOUBLES_EQUAL(UniqueCombinationsRun(13, 3, 3, false), 0.000518, 1e-7);
        UNIT_ASSERT_DOUBLES_EQUAL(UniqueCombinationsRun(113, 3, 1, false), 0.009091, 1e-7);
        UNIT_ASSERT_DOUBLES_EQUAL(UniqueCombinationsRun(113, 3, 5, false), 0.045251, 1e-7);
        UNIT_ASSERT_DOUBLES_EQUAL(UniqueCombinationsRun(113, 9, 1, false), 0.009237, 1e-7);
        UNIT_ASSERT_DOUBLES_EQUAL(UniqueCombinationsRun(113, 9, 8, false), 0.01387, 1e-7);
        UNIT_ASSERT_DOUBLES_EQUAL(UniqueCombinationsRun(13, 3, 1, true), 0.000205, 1e-7);
        UNIT_ASSERT_DOUBLES_EQUAL(UniqueCombinationsRun(13, 3, 3, true), 0.004262, 1e-7);
        UNIT_ASSERT_DOUBLES_EQUAL(UniqueCombinationsRun(113, 3, 1, true), 0.009091, 1e-7);
        UNIT_ASSERT_DOUBLES_EQUAL(UniqueCombinationsRun(113, 3, 5, true), 0.63673, 1e-7);
        UNIT_ASSERT_DOUBLES_EQUAL(UniqueCombinationsRun(113, 9, 1, true), 0.009237, 1e-7);
        UNIT_ASSERT_DOUBLES_EQUAL(UniqueCombinationsRun(113, 9, 8, true), 0.072514, 1e-7);
    }

    double UniformityRun(ui32 replicas, ui32 nToSelect, ui32 replicasInRing, bool useRingSpecificNodeSelection) {
        THashMap<TActorId, ui32> history;

        TStateStorageInfo info;
        FillStateStorageInfo(&info, replicas, nToSelect, replicasInRing, useRingSpecificNodeSelection);

        TStateStorageInfo::TSelection selection;
        for (ui64 tabletId = 8000000; tabletId < 9000000; ++tabletId) {
            info.SelectReplicas(tabletId, &selection);
            Y_ABORT_UNLESS(nToSelect == selection.Sz);
            for (ui32 idx : xrange(nToSelect))
                history[selection.SelectedReplicas[idx]] += 1;
        }

        ui32 mn = history.begin()->second;
        ui32 mx = history.begin()->second;

        for (auto &x : history) {
            const ui32 cur = x.second;
            if (cur < mn)
                mn = cur;
            if (cur > mx)
                mx = cur;
        }

        return static_cast<double>(mx - mn) / static_cast<double>(mx);
    }

    Y_UNIT_TEST(Tablet72075186224040026Test) {
        TStateStorageInfo info;
        FillStateStorageInfo(&info, 9, 5, 1, false);
        TStateStorageInfo::TSelection selection;
        info.SelectReplicas(72075186224040026UL, &selection);
        ui32 expected[] = {0, 2, 1, 3, 4};
        for (ui32 i : xrange(5)) {
            UNIT_ASSERT_EQUAL(selection.SelectedReplicas[i].NodeId(), expected[i]);
        }
    }

    Y_UNIT_TEST(NonDuplicatedNodesTest) {
        TStateStorageInfo info;
        FillStateStorageInfo(&info, 9, 5, 1, false);

        // replicate old walker behaviour locally for comparison
        auto oldSelectRingIndices = [](ui32 hash, ui32 total, ui32 nToSelect, TVector<ui32>& out) {
            NStateStorageOld::TStateStorageRingWalker walker(hash, total);
            out.resize(nToSelect);
            for (ui32 i : xrange(nToSelect)) {
                out[i] = walker.Next();
            }
        };

        ui32 good = 0;
        for (ui64 tabletId : xrange(Max<ui64>() - 1000000UL, Max<ui64>())) {
            TStateStorageInfo::TSelection selection;
            info.SelectReplicas(tabletId, &selection);

            const ui32 hash = StateStorageHashFromTabletID(tabletId);
            TVector<ui32> oldRings;
            oldSelectRingIndices(hash, 9, 5, oldRings);

            std::unordered_set<ui32> oldUnique(oldRings.begin(), oldRings.end());
            if (oldUnique.size() == 5) {
                good++;
                for (ui32 i : xrange(5)) {
                    UNIT_ASSERT_EQUAL(info.Rings[oldRings[i]].SelectReplica(hash),
                                      selection.SelectedReplicas[i]);
                }
            } else {
                ui32 same = 0;
                for (ui32 i : xrange(5)) {
                    if (info.Rings[oldRings[i]].SelectReplica(hash) == selection.SelectedReplicas[i]) {
                        same++;
                    }
                }
                UNIT_ASSERT_EQUAL(same, oldUnique.size());
            }
        }
        UNIT_ASSERT_EQUAL(good, 999941);
    }

    Y_UNIT_TEST(DuplicatedNodesTest) {
        TStateStorageInfo info;
        FillStateStorageInfo(&info, 9, 5, 1, false);
        ui32 bad = 0;
        for (ui64 tabletId : xrange(Max<ui64>() - 1000000UL, Max<ui64>())) {
            TStateStorageInfo::TSelection selection;
            info.SelectReplicas(tabletId, &selection);
            std::unordered_set<TActorId> nodes;
            for (ui32 i : xrange(5)) {
                nodes.insert(selection.SelectedReplicas[i]);
            }
            if (nodes.size() != 5) {
                bad++;
            }
        }
        UNIT_ASSERT_EQUAL(bad, 0);
    }

    Y_UNIT_TEST(UniformityTest) {
        UNIT_ASSERT(UniformityRun(13, 3, 1, false) < 0.10);
        UNIT_ASSERT(UniformityRun(13, 3, 3, false) < 0.10);
        UNIT_ASSERT(UniformityRun(113, 3, 1, false) < 0.10);
        UNIT_ASSERT(UniformityRun(113, 3, 5, false) < 0.10);
        UNIT_ASSERT(UniformityRun(113, 9, 1, false) < 0.10);
        UNIT_ASSERT(UniformityRun(113, 9, 8, false) < 0.10);
        UNIT_ASSERT(UniformityRun(13, 3, 1, true) < 0.10);
        UNIT_ASSERT(UniformityRun(13, 3, 3, true) < 0.10);
        UNIT_ASSERT(UniformityRun(113, 3, 1, true) < 0.10);
        UNIT_ASSERT(UniformityRun(113, 3, 5, true) < 0.10);
        UNIT_ASSERT(UniformityRun(113, 9, 1, true) < 0.10);
        UNIT_ASSERT(UniformityRun(113, 9, 8, true) < 0.10);
    }
}

}
