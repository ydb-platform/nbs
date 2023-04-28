#include "target.h"

#include "device.h"
#include "env_impl.h"
#include "spdk.h"

#include <util/generic/map.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NSpdk {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

using spdk_nvmf_subsystem_ptr_t = std::unique_ptr<
    spdk_nvmf_subsystem,
    decltype(&spdk_nvmf_subsystem_destroy)
>;

////////////////////////////////////////////////////////////////////////////////

struct TStateChangedCompletion
{
    TPromise<void> Result;

    TStateChangedCompletion(TPromise<void> result)
        : Result(std::move(result))
    {}

    static void Callback(spdk_nvmf_subsystem* subsystem, void* cb_arg, int error)
    {
        std::unique_ptr<TStateChangedCompletion> completion(
            reinterpret_cast<TStateChangedCompletion*>(cb_arg));

        if (error) {
            completion->Result.SetException(TStringBuilder()
                << "unable to change subsystem state "
                << spdk_nvmf_subsystem_get_nqn(subsystem));
        } else {
            completion->Result.SetValue();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TAddTransportCompletion
{
    TPromise<void> Result;
    TString TransportId;

    TAddTransportCompletion(TPromise<void> result, TString transportId)
        : Result(std::move(result))
        , TransportId(std::move(transportId))
    {}

    static void Callback(void* ctx, int error)
    {
        std::unique_ptr<TAddTransportCompletion> completion(
            reinterpret_cast<TAddTransportCompletion*>(ctx));

        if (error) {
            completion->Result.SetException(TStringBuilder()
                << "unable to configure listener for NVMe transport ID "
                << completion->TransportId.Quote());
        } else {
            completion->Result.SetValue();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TListenCompletion
{
    TPromise<void> Result;
    TString TransportId;

    TListenCompletion(TPromise<void> result, TString transportId)
        : Result(std::move(result))
        , TransportId(std::move(transportId))
    {}

    static void Callback(void* ctx, int error)
    {
        std::unique_ptr<TListenCompletion> completion(
            reinterpret_cast<TListenCompletion*>(ctx));

        if (error) {
            completion->Result.SetException(TStringBuilder()
                << "unable to configure listener for NVMe transport ID "
                << completion->TransportId.Quote());
        } else {
            completion->Result.SetValue();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

using TSpdkDeviceMap = TMap<TString, ISpdkDevicePtr>;

////////////////////////////////////////////////////////////////////////////////

class TSpdkNVMeTarget final
    : public ISpdkTarget
{
private:
    TSpdkEnvPtr Env;
    spdk_nvmf_subsystem_ptr_t Subsystem;
    TSpdkDeviceMap Devices;

    TFuture<void> StopResult;

public:
    TSpdkNVMeTarget(
            TSpdkEnvPtr env,
            spdk_nvmf_subsystem_ptr_t subsystem,
            TSpdkDeviceMap devices)
        : Env(std::move(env))
        , Subsystem(std::move(subsystem))
        , Devices(std::move(devices))
    {}

    void Start() override;
    void Stop() override;

    TFuture<void> StartAsync() override;
    TFuture<void> StopAsync() override;

    ISpdkDevicePtr GetDevice(const TString& name) override;

private:
    using TChangeState = int(
        spdk_nvmf_subsystem*,
        spdk_nvmf_subsystem_state_change_done,
        void*);

    TFuture<void> ChangeState(TChangeState fn);
    TFuture<void> StopDevices();
};

////////////////////////////////////////////////////////////////////////////////

void TSpdkNVMeTarget::Start()
{
    StartAsync().GetValue(StartTimeout);
}

void TSpdkNVMeTarget::Stop()
{
    StopAsync().GetValue(StartTimeout);
}

TFuture<void> TSpdkNVMeTarget::StartAsync()
{
    return ChangeState(spdk_nvmf_subsystem_start);
}

TFuture<void> TSpdkNVMeTarget::StopAsync()
{
    if (!StopResult.HasValue()) {
        StopResult = StopDevices()
            .Apply([this] (const auto&) {
                return ChangeState(spdk_nvmf_subsystem_stop);
            })
            .Apply([this] (const auto&) {
                return Env->Execute([=] {
                    return Subsystem.reset();
                });
            });
    }
    return StopResult;
}

TFuture<void> TSpdkNVMeTarget::StopDevices()
{
    TVector<TFuture<void>> stopResults;

    for (auto& it: Devices) {
        stopResults.push_back(it.second->StopAsync());
    }

    return WaitAll(stopResults);
}

TFuture<void> TSpdkNVMeTarget::ChangeState(TChangeState fn)
{
    return Env->Execute([=] {
        auto result = NewPromise();

        auto completion = std::make_unique<TStateChangedCompletion>(result);
        int error = fn(
            Subsystem.get(),
            TStateChangedCompletion::Callback,
            completion.get());

        if (error) {
            ythrow TServiceError(MAKE_SYSTEM_ERROR(error))
                << "unable to change subsystem state "
                << spdk_nvmf_subsystem_get_nqn(Subsystem.get());
        }

        completion.release();   // will be deleted in callback
        return result.GetFuture();
    });
}

ISpdkDevicePtr TSpdkNVMeTarget::GetDevice(const TString& name)
{
    auto it = Devices.find(name);
    return it != Devices.end() ? it->second : nullptr;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TSpdkEnv::AddTransport(const TString& transportId)
{
    return Execute([=] {
        spdk_nvme_transport_id trid;
        Zero(trid);

        int error = spdk_nvme_transport_id_parse(&trid, transportId.c_str());
        if (error) {
            ythrow TServiceError(MAKE_SYSTEM_ERROR(error))
                << "unable to parse NVMe transport ID " << transportId.Quote();
        }

        spdk_nvmf_transport_opts opts;
        if (!spdk_nvmf_transport_opts_init(trid.trstring, &opts, sizeof(opts))) {
            ythrow TServiceError(E_NOT_IMPLEMENTED)
                << "unable to parse NVMe transport ID " << transportId.Quote()
                << " - unsupported transport type " << trid.trstring;
        }

        auto* transport = spdk_nvmf_transport_create(trid.trstring, &opts);
        if (!transport) {
            ythrow TServiceError(E_NOT_IMPLEMENTED)
                << "unable to parse NVMe transport ID " << transportId.Quote()
                << " - unable to create transport " << trid.trstring;
        }

        auto* target = spdk_nvmf_get_tgt(nullptr);
        if (!target) {
            ythrow TServiceError(E_NOT_IMPLEMENTED)
                << "multiple targets are not supported";
        }

        auto result = NewPromise();

        auto completion = std::make_unique<TAddTransportCompletion>(result, transportId);
        spdk_nvmf_tgt_add_transport(
            target,
            transport,
            TAddTransportCompletion::Callback,
            completion.get());

        completion.release();   // will be deleted in callback
        return result.GetFuture();
    });
}

TFuture<void> TSpdkEnv::StartListen(const TString& transportId)
{
    return Execute([=] {
        spdk_nvme_transport_id trid;
        Zero(trid);

        int error = spdk_nvme_transport_id_parse(&trid, transportId.c_str());
        if (error) {
            ythrow TServiceError(MAKE_SYSTEM_ERROR(error))
                << "unable to parse NVMe transport ID " << transportId.Quote();
        }

        if (!spdk_nvme_transport_available(trid.trtype)) {
            ythrow TServiceError(E_NOT_IMPLEMENTED)
                << "unable to parse NVMe transport ID " << transportId.Quote()
                << " - unsupported transport type " << trid.trstring;
        }

        auto* target = spdk_nvmf_get_tgt(nullptr);
        if (!target) {
            ythrow TServiceError(E_NOT_IMPLEMENTED)
                << "multiple targets are not supported";
        }

        spdk_nvmf_listen_opts opts;
        spdk_nvmf_listen_opts_init(&opts, sizeof(opts));

        error = spdk_nvmf_tgt_listen_ext(target, &trid, &opts);
        if (error) {
            ythrow TServiceError(MAKE_SYSTEM_ERROR(error))
                << "unable to configure listener for NVMe transport ID "
                << transportId.Quote();
        }
    });
}

TFuture<ISpdkTargetPtr> TSpdkEnv::CreateNVMeTarget(
    const TString& nqn,
    const TVector<TString>& devices,
    const TVector<TString>& transportIds)
{
    auto env = shared_from_this();
    return Execute([=] () {
        auto* target = spdk_nvmf_get_tgt(nullptr);
        if (!target) {
            ythrow TServiceError(E_NOT_IMPLEMENTED)
                << "multiple targets are not supported";
        }

        spdk_nvmf_subsystem_ptr_t subsystem(
            spdk_nvmf_subsystem_create(
                target,
                nqn.c_str(),
                SPDK_NVMF_SUBTYPE_NVME,
                devices.size()),
            spdk_nvmf_subsystem_destroy);

        if (!subsystem) {
            ythrow TServiceError(E_ARGUMENT)
                << "unable to create NVMe-oF subsystem " << nqn.Quote();
        }

        TSpdkDeviceMap bdevs;
        for (const auto& name: devices) {
            auto* bdev = spdk_bdev_get_by_name(name.c_str());
            if (!bdev) {
                ythrow TServiceError(E_ARGUMENT)
                    << "unable to find device " << name.Quote();
            }

            // spdk_nvmf_subsystem_add_ns will claim device to prevent its direct usage
            auto device = OpenDevice(bdev, true);

            spdk_nvmf_ns_opts ns_opts;
            spdk_nvmf_ns_opts_get_defaults(&ns_opts, sizeof(ns_opts));

            ui32 nsid = spdk_nvmf_subsystem_add_ns_ext(
                subsystem.get(),
                name.c_str(),
                &ns_opts,
                sizeof(ns_opts),
                nullptr);   // ptpl_file

            if (!nsid) {
                ythrow TServiceError(E_ARGUMENT)
                    << "unable to bind subsystem to device " << name.Quote();
            }

            bdevs.emplace(name, std::move(device));
        }

        TVector<TFuture<void>> futures;

        for (const auto& transportId: transportIds) {
            spdk_nvme_transport_id trid;
            Zero(trid);

            int error = spdk_nvme_transport_id_parse(&trid, transportId.c_str());
            if (error) {
                ythrow TServiceError(MAKE_SYSTEM_ERROR(error))
                    << "unable to parse NVMe transport ID "
                    << transportId.Quote();
            }

            auto result = NewPromise();
            auto completion = std::make_unique<TListenCompletion>(result, transportId);

            spdk_nvmf_subsystem_add_listener(
                subsystem.get(),
                &trid,
                TListenCompletion::Callback,
                completion.release());

            futures.push_back(result.GetFuture());
        }

        return WaitExceptionOrAll(futures).Apply(
            [&] (const auto& future) -> ISpdkTargetPtr {
                future.TryRethrow();

                spdk_nvmf_subsystem_set_allow_any_host(subsystem.get(), true);

                return std::make_shared<TSpdkNVMeTarget>(
                    env,
                    std::move(subsystem),
                    std::move(bdevs));
            });
    });
}

}   // namespace NCloud::NBlockStore::NSpdk
