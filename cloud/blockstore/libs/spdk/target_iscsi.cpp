#include "target.h"

#include "device.h"
#include "env_impl.h"
#include "spdk.h"

#include <util/generic/map.h>
#include <util/string/cast.h>

namespace NCloud::NBlockStore::NSpdk {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

using spdk_iscsi_portal_ptr_t = std::unique_ptr<
    spdk_iscsi_portal,
    decltype(&iscsi_portal_destroy)
>;

using spdk_iscsi_portal_grp_ptr_t = std::unique_ptr<
    spdk_iscsi_portal_grp,
    decltype(&iscsi_portal_grp_release)
>;

using TSpdkDeviceMap = TMap<TString, ISpdkDevicePtr>;

////////////////////////////////////////////////////////////////////////////////

class TSpdkSCSITarget final
    : public ISpdkTarget
{
private:
    TSpdkEnvPtr Env;
    spdk_iscsi_tgt_node* Target;
    TSpdkDeviceMap Devices;

public:
    TSpdkSCSITarget(
            TSpdkEnvPtr env,
            spdk_iscsi_tgt_node* target,
            TSpdkDeviceMap devices)
        : Env(std::move(env))
        , Target(target)
        , Devices(std::move(devices))
    {}

    void Start() override;
    void Stop() override;

    TFuture<void> StartAsync() override;
    TFuture<void> StopAsync() override;

    ISpdkDevicePtr GetDevice(const TString& name) override;
};

////////////////////////////////////////////////////////////////////////////////

void TSpdkSCSITarget::Start()
{
    StartAsync().GetValue(StartTimeout);
}

void TSpdkSCSITarget::Stop()
{
    StopAsync().GetValue(StartTimeout);
}

TFuture<void> TSpdkSCSITarget::StartAsync()
{
    Y_UNUSED(Target);

    // TODO
    return MakeFuture();
}

TFuture<void> TSpdkSCSITarget::StopAsync()
{
    TVector<TFuture<void>> stopResults;

    for (auto& it: Devices) {
        stopResults.push_back(it.second->StopAsync());
    }

    return WaitAll(stopResults);
}

ISpdkDevicePtr TSpdkSCSITarget::GetDevice(const TString& name)
{
    auto it = Devices.find(name);
    return it != Devices.end() ? it->second : nullptr;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TSpdkEnv::CreatePortalGroup(
    ui32 tag,
    const TVector<TPortal>& portals)
{
    return Execute([=] {
        constexpr bool is_private = false;
        constexpr bool wait_open = false;

        spdk_iscsi_portal_grp_ptr_t group(
            iscsi_portal_grp_create(tag, is_private),
            iscsi_portal_grp_release);

        if (!group) {
            ythrow TServiceError(E_ARGUMENT)
                << "unable to create iSCSI portal group #" << tag;
        }

        for (const auto& [listenAddress, listenPort]: portals) {
            auto listenPortStr = ToString(listenPort);  // LAME

            spdk_iscsi_portal_ptr_t portal(
                iscsi_portal_create(listenAddress.c_str(), listenPortStr.c_str()),
                iscsi_portal_destroy);

            if (!portal) {
                ythrow TServiceError(E_ARGUMENT)
                    << "unable to create iSCSI portal " << listenAddress.Quote();
            }

            iscsi_portal_grp_add_portal(group.get(), portal.get());

            portal.release();   // ownership transferred to spdk
        }

        int error = iscsi_portal_grp_open(group.get(), wait_open);
        if (error) {
            ythrow TServiceError(MAKE_SYSTEM_ERROR(error))
                << "unable to open iSCSI portal group #" << tag;
        }

        error = iscsi_portal_grp_register(group.get());
        if (error) {
            ythrow TServiceError(MAKE_SYSTEM_ERROR(error))
                << "unable to register iSCSI portal group #" << tag;
        }

        group.release();   // ownership transferred to spdk
    });
}

TFuture<void> TSpdkEnv::CreateInitiatorGroup(
    ui32 tag,
    const TVector<TInitiator>& initiators)
{
    return Execute([=] {
        TVector<const char*> names(Reserve(initiators.size()));
        TVector<const char*> netmasks(Reserve(initiators.size()));

        for (const auto& [name, netmask]: initiators) {
            names.push_back(name.c_str());
            netmasks.push_back(netmask.c_str());
        }

        int error = iscsi_init_grp_create_from_initiator_list(
            tag,
            names.size(),
            (char **)names.begin(),
            netmasks.size(),
            (char **)netmasks.begin());

        if (error) {
            ythrow TServiceError(MAKE_SYSTEM_ERROR(error))
                << "unable to register iSCSI initiator group #" << tag;
        }
    });
}

TFuture<ISpdkTargetPtr> TSpdkEnv::CreateSCSITarget(
    const TString& name,
    const TVector<TDevice>& devices,
    const TVector<TGroupMapping>& groups)
{
    auto env = shared_from_this();
    return Execute([=] () -> ISpdkTargetPtr {
        TSpdkDeviceMap bdevs;

        TVector<const char*> names(Reserve(devices.size()));
        TVector<int> luns(Reserve(devices.size()));

        for (const auto& [name, lun]: devices) {
            auto* bdev = spdk_bdev_get_by_name(name.c_str());
            if (!bdev) {
                ythrow TServiceError(E_ARGUMENT)
                    << "unable to find device " << name.Quote();
            }

            // iscsi_tgt_node_construct will claim device to prevent its direct usage
            bdevs.emplace(name, OpenDevice(bdev, true));

            names.push_back(name.c_str());
            luns.push_back(lun);
        }

        TVector<int> pgs(Reserve(groups.size()));
        TVector<int> igs(Reserve(groups.size()));

        for (const auto& [ig, pg]: groups) {
            igs.push_back(ig);
            pgs.push_back(pg);
        }

        constexpr int queue_depth = 64; // DEFAULT_MAX_QUEUE_DEPTH
        constexpr bool disable_chap = true;
        constexpr bool require_chap = false;
        constexpr bool mutual_chap = false;
        constexpr int chap_group = 0;
        constexpr bool header_digest = false;
        constexpr bool data_digest = false;

        auto* target = iscsi_tgt_node_construct(
            -1,             // target index (assign automatically)
            name.c_str(),
            nullptr,        // alias
            pgs.begin(),
            igs.begin(),
            groups.size(),
            names.begin(),
            luns.begin(),
            devices.size(),
            queue_depth,
            disable_chap,
            require_chap,
            mutual_chap,
            chap_group,
            header_digest,
            data_digest);

        if (!target) {
            ythrow TServiceError(E_ARGUMENT)
                << "unable to register iSCSI target " << name.Quote();
        }

        return std::make_shared<TSpdkSCSITarget>(
            std::move(env),
            target,
            std::move(bdevs));
    });
}

}   // namespace NCloud::NBlockStore::NSpdk
