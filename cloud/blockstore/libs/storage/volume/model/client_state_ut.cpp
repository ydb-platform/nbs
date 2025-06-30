#include "client_state.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/generic/guid.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TVolumeClientInfo CreateVolumeClientInfo(
    NProto::EVolumeAccessMode accessMode,
    NProto::EVolumeMountMode mountMode,
    ui32 mountFlags)
{
    NProto::TVolumeClientInfo info;
    info.SetClientId(CreateGuidAsString());
    info.SetVolumeAccessMode(accessMode);
    info.SetVolumeMountMode(mountMode);
    info.SetMountFlags(mountFlags);
    return info;
}

TActorId CreateActor(ui32 nodeId, ui64 id)
{
    return TActorId(nodeId, 0, id, 0);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVolumeClientStateTest)
{
    Y_UNIT_TEST(ShouldAddPipe)
    {
        auto initialMountMode = NProto::VOLUME_MOUNT_LOCAL;
        auto initialAccessMode = NProto::VOLUME_ACCESS_READ_WRITE;
        const ui64 mountFlags = 0;

        auto info = CreateVolumeClientInfo(
            initialAccessMode,
            initialMountMode,
            mountFlags);

        TVolumeClientState client {info};

        auto res = client.AddPipe(
            CreateActor(1, 1),
            CreateActor(1, 1).NodeId(),
            initialAccessMode,
            initialMountMode,
            mountFlags);
        // should be false since pipe settings
        // correspond to client settings
        UNIT_ASSERT_C(!HasError(res.Error), FormatError(res.Error));
        UNIT_ASSERT_VALUES_EQUAL(true, res.IsNew);
        UNIT_ASSERT_VALUES_EQUAL(1, client.GetPipes().size());

        res = client.AddPipe(
            CreateActor(1, 1),
            CreateActor(1, 1).NodeId(),
            initialAccessMode,
            initialMountMode,
            mountFlags);
        // nothing changed
        UNIT_ASSERT_C(!HasError(res.Error), FormatError(res.Error));
        UNIT_ASSERT_VALUES_EQUAL(false, res.IsNew);
        UNIT_ASSERT_VALUES_EQUAL(1, client.GetPipes().size());

        res = client.AddPipe(
            CreateActor(1, 1),
            CreateActor(1, 1).NodeId(),
            NProto::VOLUME_ACCESS_READ_ONLY,
            initialMountMode,
            mountFlags);
        // changed AccessMode -> return true
        UNIT_ASSERT_C(!HasError(res.Error), FormatError(res.Error));
        UNIT_ASSERT_VALUES_EQUAL(true, res.IsNew);
        UNIT_ASSERT_VALUES_EQUAL(1, client.GetPipes().size());

        // add one more pipe
        res = client.AddPipe(
            CreateActor(2, 2),
            CreateActor(2, 2).NodeId(),
            NProto::VOLUME_ACCESS_READ_WRITE,
            initialMountMode,
            mountFlags);
        // changed -> return true
        UNIT_ASSERT_C(!HasError(res.Error), FormatError(res.Error));
        UNIT_ASSERT_VALUES_EQUAL(true, res.IsNew);
        UNIT_ASSERT_VALUES_EQUAL(2, client.GetPipes().size());
    }

    Y_UNIT_TEST(ShouldProperlyUpdateClientInfoWhenAddingAndDeleting)
    {
        auto initialMountMode = NProto::VOLUME_MOUNT_REMOTE;
        auto initialAccessMode = NProto::VOLUME_ACCESS_READ_ONLY;
        const ui64 mountFlags = 0;

        auto info = CreateVolumeClientInfo(
            initialAccessMode,
            initialMountMode,
            0);

        TVolumeClientState client {info};

        auto res = client.AddPipe(
            CreateActor(1, 1),
            CreateActor(1, 1).NodeId(),
            initialAccessMode,
            initialMountMode,
            mountFlags);
        UNIT_ASSERT_C(!HasError(res.Error), FormatError(res.Error));
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::VOLUME_MOUNT_REMOTE,
            client.GetVolumeClientInfo().GetVolumeMountMode());
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::VOLUME_ACCESS_READ_ONLY,
            client.GetVolumeClientInfo().GetVolumeAccessMode());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            client.GetVolumeClientInfo().GetMountFlags());

        // add rw
        res = client.AddPipe(
            CreateActor(2, 2),
            CreateActor(2, 2).NodeId(),
            NProto::VOLUME_ACCESS_READ_WRITE,
            initialMountMode,
            mountFlags);
        UNIT_ASSERT_C(!HasError(res.Error), FormatError(res.Error));
        UNIT_ASSERT_VALUES_EQUAL(true, res.IsNew);
        UNIT_ASSERT_VALUES_EQUAL(2, client.GetPipes().size());
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::VOLUME_MOUNT_REMOTE,
            client.GetVolumeClientInfo().GetVolumeMountMode());
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::VOLUME_ACCESS_READ_WRITE,
            client.GetVolumeClientInfo().GetVolumeAccessMode());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            client.GetVolumeClientInfo().GetMountFlags());

        // add local mount
        res = client.AddPipe(
            CreateActor(3, 3),
            CreateActor(3, 3).NodeId(),
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            mountFlags);
        UNIT_ASSERT_C(!HasError(res.Error), FormatError(res.Error));
        UNIT_ASSERT_VALUES_EQUAL(true, res.IsNew);
        UNIT_ASSERT_VALUES_EQUAL(3, client.GetPipes().size());
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::VOLUME_MOUNT_LOCAL,
            client.GetVolumeClientInfo().GetVolumeMountMode());
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::VOLUME_ACCESS_READ_WRITE,
            client.GetVolumeClientInfo().GetVolumeAccessMode());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            client.GetVolumeClientInfo().GetMountFlags());

        // remove rw
        client.RemovePipe(CreateActor(2, 2), TInstant());
        UNIT_ASSERT_VALUES_EQUAL(2, client.GetPipes().size());
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::VOLUME_MOUNT_LOCAL,
            client.GetVolumeClientInfo().GetVolumeMountMode());
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::VOLUME_ACCESS_READ_WRITE,
            client.GetVolumeClientInfo().GetVolumeAccessMode());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            client.GetVolumeClientInfo().GetMountFlags());

        // remove read-only
        client.RemovePipe(CreateActor(1, 1), TInstant());
        UNIT_ASSERT_VALUES_EQUAL(1, client.GetPipes().size());
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::VOLUME_MOUNT_LOCAL,
            client.GetVolumeClientInfo().GetVolumeMountMode());
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::VOLUME_ACCESS_READ_WRITE,
            client.GetVolumeClientInfo().GetVolumeAccessMode());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            client.GetVolumeClientInfo().GetMountFlags());

        // remove local
        client.RemovePipe(CreateActor(3, 3), TInstant());
        UNIT_ASSERT_VALUES_EQUAL(0, client.GetPipes().size());
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::VOLUME_MOUNT_REMOTE,
            client.GetVolumeClientInfo().GetVolumeMountMode());
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::VOLUME_ACCESS_READ_WRITE,
            client.GetVolumeClientInfo().GetVolumeAccessMode());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            client.GetVolumeClientInfo().GetMountFlags());
    }

    Y_UNIT_TEST(ShouldHandlePipeActivationDeactivation)
    {
        auto initialMountMode = NProto::VOLUME_MOUNT_REMOTE;
        auto initialAccessMode = NProto::VOLUME_ACCESS_READ_ONLY;
        const ui64 mountFlags = 0;

        auto info =
            CreateVolumeClientInfo(initialAccessMode, initialMountMode, 0);

        TVolumeClientState client{info};

        {
            // When we have no pipes accessMode should be obtained from
            // clientInfo for local and remote clients.

            NProto::TError ans;
            ans = client.CheckLocalRequest(1, true, "", "");
            UNIT_ASSERT_C(HasError(ans), FormatError(ans));

            ans = client.CheckLocalRequest(1, false, "", "");
            UNIT_ASSERT_C(!HasError(ans), FormatError(ans));

            ans = client.CheckPipeRequest(CreateActor(1, 2), true, "", "");
            UNIT_ASSERT_C(HasError(ans), FormatError(ans));

            ans = client.CheckPipeRequest(CreateActor(1, 2), false, "", "");
            UNIT_ASSERT_C(!HasError(ans), FormatError(ans));
        }

        {
            // Add local pipe. AccessMode should be updated upon AddPipe.
            auto res = client.AddPipe(
                CreateActor(1, 1),
                CreateActor(1, 1).NodeId(),
                NProto::VOLUME_ACCESS_READ_WRITE,
                NProto::VOLUME_MOUNT_LOCAL,
                mountFlags);
            // changed -> return true
            UNIT_ASSERT_C(!HasError(res.Error), FormatError(res.Error));
            UNIT_ASSERT_VALUES_EQUAL(true, res.IsNew);
            UNIT_ASSERT_VALUES_EQUAL(1, client.GetPipes().size());

            // Local requests should be permitted.
            NProto::TError ans;
            ans = client.CheckLocalRequest(1, true, "", "");
            UNIT_ASSERT_C(!HasError(ans), FormatError(ans));

            ans = client.CheckLocalRequest(1, false, "", "");
            UNIT_ASSERT_C(!HasError(ans), FormatError(ans));

            // Remote requests should be declined.
            ans = client.CheckPipeRequest(CreateActor(1, 2), true, "", "");
            UNIT_ASSERT_C(HasError(ans), FormatError(ans));

            ans = client.CheckPipeRequest(CreateActor(1, 2), false, "", "");
            UNIT_ASSERT_C(HasError(ans), FormatError(ans));
        }

        {
            // Add remote pipe.
            auto res = client.AddPipe(
                CreateActor(2, 2),
                CreateActor(2, 2).NodeId(),
                NProto::VOLUME_ACCESS_READ_WRITE,
                NProto::VOLUME_MOUNT_REMOTE,
                mountFlags);

            UNIT_ASSERT_C(!HasError(res.Error), FormatError(res.Error));
            UNIT_ASSERT_VALUES_EQUAL(true, res.IsNew);
            UNIT_ASSERT_VALUES_EQUAL(2, client.GetPipes().size());

            NProto::TError ans;
            // Local requests should be still permitted.
            ans = client.CheckLocalRequest(1, true, "", "");
            UNIT_ASSERT_C(!HasError(ans), FormatError(ans));

            ans = client.CheckLocalRequest(1, false, "", "");
            UNIT_ASSERT_C(!HasError(ans), FormatError(ans));

            // Remote requests should be rejected since local pipe in active state.
            ans = client.CheckPipeRequest(CreateActor(2, 2), true, "", "");
            UNIT_ASSERT_VALUES_EQUAL_C(E_REJECTED, ans.GetCode(), FormatError(ans));

            ans = client.CheckPipeRequest(CreateActor(2, 2), false, "", "");
            UNIT_ASSERT_VALUES_EQUAL_C(E_REJECTED, ans.GetCode(), FormatError(ans));

            // Remove local pipe.
            client.RemovePipe(CreateActor(1, 1), TInstant());

            // Local requests should now declined.
            ans = client.CheckLocalRequest(1, true, "", "");
            UNIT_ASSERT_C(HasError(ans), FormatError(ans));

            ans = client.CheckLocalRequest(1, false, "", "");
            UNIT_ASSERT_C(HasError(ans), FormatError(ans));

            // Remote requests now permited and should activate remote pipe.
            ans = client.CheckPipeRequest(CreateActor(2, 2), true, "", "");
            UNIT_ASSERT_C(!HasError(ans), FormatError(ans));

            ans = client.CheckPipeRequest(CreateActor(2, 2), false, "", "");
            UNIT_ASSERT_C(!HasError(ans), FormatError(ans));
        }

        {
            // Create new local pipe should success
            auto res = client.AddPipe(
                CreateActor(3, 3),
                CreateActor(3, 3).NodeId(),
                NProto::VOLUME_ACCESS_READ_WRITE,
                NProto::VOLUME_MOUNT_LOCAL,
                mountFlags);
            // changed -> return true
            UNIT_ASSERT_C(!HasError(res.Error), FormatError(res.Error));
            UNIT_ASSERT_VALUES_EQUAL(true, res.IsNew);
            UNIT_ASSERT_VALUES_EQUAL(2, client.GetPipes().size());

            NProto::TError ans;
            // Remote requests should be permitted since local pipe not
            // activated yet.
            ans = client.CheckPipeRequest(CreateActor(2, 2), true, "", "");
            UNIT_ASSERT_C(!HasError(ans), FormatError(ans));

            ans = client.CheckPipeRequest(CreateActor(2, 2), false, "", "");
            UNIT_ASSERT_C(!HasError(ans), FormatError(ans));

            // Local requests should be permitted.
            ans = client.CheckLocalRequest(3, true, "", "");
            UNIT_ASSERT_C(!HasError(ans), FormatError(ans));

            ans = client.CheckLocalRequest(3, false, "", "");
            UNIT_ASSERT_C(!HasError(ans), FormatError(ans));

            // Remote requests should be declined since local pipe is activated.
            ans = client.CheckPipeRequest(CreateActor(2, 2), true, "", "");
            UNIT_ASSERT_C(HasError(ans), FormatError(ans));

            ans = client.CheckPipeRequest(CreateActor(2, 2), false, "", "");
            UNIT_ASSERT_C(HasError(ans), FormatError(ans));
        }

        {
            // try to reactivate remote pipe -> should fail
            auto res = client.AddPipe(
                CreateActor(2, 2),
                CreateActor(2, 2).NodeId(),
                NProto::VOLUME_ACCESS_READ_WRITE,
                NProto::VOLUME_MOUNT_REMOTE,
                mountFlags);
            // changed -> return true
            UNIT_ASSERT_C(HasError(res.Error), FormatError(res.Error));
        }
    }

    Y_UNIT_TEST(ShouldPreserveAccessModeOnPipeDisconnect)
    {
        auto initialMountMode = NProto::VOLUME_MOUNT_LOCAL;
        auto initialAccessMode = NProto::VOLUME_ACCESS_READ_WRITE;
        const ui64 mountFlags = 0;

        auto info = CreateVolumeClientInfo(
            initialAccessMode,
            initialMountMode,
            0);

        TVolumeClientState client {info};

        NProto::TError ans;

        // No pipes exist. Request OK.
        ans = client.CheckLocalRequest(1, true, "", "");
        UNIT_ASSERT_C(!HasError(ans), FormatError(ans));
        ans = client.CheckPipeRequest(CreateActor(2, 2), false, "", "");
        UNIT_ASSERT_C(!HasError(ans), FormatError(ans));

        // Add and remove remote pipe without activation.
        client.AddPipe(
            CreateActor(3, 3),
            CreateActor(3, 3).NodeId(),
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            mountFlags);
        client.RemovePipe(CreateActor(3, 3), TInstant());

        // No pipes exist. Request OK.
        ans = client.CheckLocalRequest(1, true, "", "");
        UNIT_ASSERT_C(!HasError(ans), FormatError(ans));
        ans = client.CheckPipeRequest(CreateActor(2, 2), false, "", "");
        UNIT_ASSERT_C(!HasError(ans), FormatError(ans));
    }

    Y_UNIT_TEST(ShouldSelectNewActiveLocalPipeOnReconnect)
    {
        auto initialMountMode = NProto::VOLUME_MOUNT_LOCAL;
        auto initialAccessMode = NProto::VOLUME_ACCESS_READ_WRITE;
        const ui64 mountFlags = 0;

        auto info = CreateVolumeClientInfo(
            initialAccessMode,
            initialMountMode,
            0);

        TVolumeClientState client {info};

        NProto::TError ans;

        ans = client.CheckLocalRequest(1, true, "", "");
        UNIT_ASSERT_C(!FAILED(ans.GetCode()), ans.GetMessage());

        client.AddPipe(
            CreateActor(4, 4),
            CreateActor(4, 4).NodeId(),
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            mountFlags);

        ans = client.CheckLocalRequest(4, true, "", "");
        UNIT_ASSERT_C(!FAILED(ans.GetCode()), ans.GetMessage());
        UNIT_ASSERT_VALUES_EQUAL(1, client.GetPipes().size());
        auto pipeInfo = client.GetPipeInfo(CreateActor(4, 4));
        UNIT_ASSERT_C(pipeInfo.has_value(), "Pipe not found");
        UNIT_ASSERT_VALUES_EQUAL(
            pipeInfo->State,
            TVolumeClientState::EPipeState::ACTIVE);
    }
}

}   // namespace NCloud::NBlockStore::NStorage

template <>
inline void Out<NCloud::NBlockStore::NStorage::TVolumeClientState::EPipeState>(
    IOutputStream& out,
    NCloud::NBlockStore::NStorage::TVolumeClientState::EPipeState state)
{
    out << (ui32)state;
}

template <>
inline void Out<NCloud::NBlockStore::NProto::EVolumeMountMode>(
    IOutputStream& out,
    NCloud::NBlockStore::NProto::EVolumeMountMode mountMode)
{
    out << (ui32)mountMode;
}

template <>
inline void Out<NCloud::NBlockStore::NProto::EVolumeAccessMode>(
    IOutputStream& out,
    NCloud::NBlockStore::NProto::EVolumeAccessMode accessMode)
{
    out << (ui32)accessMode;
}
