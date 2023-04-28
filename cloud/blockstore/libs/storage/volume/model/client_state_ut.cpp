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
            false);
        // should be false since pipe settings
        // correspond to client settings
        UNIT_ASSERT_C(!FAILED(res.Error.GetCode()), res.Error.GetMessage());
        UNIT_ASSERT_VALUES_EQUAL(true, res.IsNew);
        UNIT_ASSERT_VALUES_EQUAL(1, client.GetPipes().size());

        res = client.AddPipe(
            CreateActor(1, 1),
            CreateActor(1, 1).NodeId(),
            initialAccessMode,
            initialMountMode,
            false);
        // nothing changed
        UNIT_ASSERT_C(!FAILED(res.Error.GetCode()), res.Error.GetMessage());
        UNIT_ASSERT_VALUES_EQUAL(false, res.IsNew);
        UNIT_ASSERT_VALUES_EQUAL(1, client.GetPipes().size());

        res = client.AddPipe(
            CreateActor(1, 1),
            CreateActor(1, 1).NodeId(),
            NProto::VOLUME_ACCESS_READ_ONLY,
            initialMountMode,
            false);
        // changed -> return true
        UNIT_ASSERT_C(!FAILED(res.Error.GetCode()), res.Error.GetMessage());
        UNIT_ASSERT_VALUES_EQUAL(true, res.IsNew);
        UNIT_ASSERT_VALUES_EQUAL(1, client.GetPipes().size());

        // add one more pipe
        res = client.AddPipe(
            CreateActor(2, 2),
            CreateActor(2, 2).NodeId(),
            NProto::VOLUME_ACCESS_READ_WRITE,
            initialMountMode,
            false);
        // changed -> return true
        UNIT_ASSERT_C(!FAILED(res.Error.GetCode()), res.Error.GetMessage());
        UNIT_ASSERT_VALUES_EQUAL(true, res.IsNew);
        UNIT_ASSERT_VALUES_EQUAL(2, client.GetPipes().size());
    }

    Y_UNIT_TEST(ShouldProperlyUpdateClientInfoWhenAddingAndDeleting)
    {
        auto initialMountMode = NProto::VOLUME_MOUNT_REMOTE;
        auto initialAccessMode = NProto::VOLUME_ACCESS_READ_ONLY;

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
            false);
        UNIT_ASSERT_C(!FAILED(res.Error.GetCode()), res.Error.GetMessage());
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
            false);
        UNIT_ASSERT_C(!FAILED(res.Error.GetCode()), res.Error.GetMessage());
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

        // add locals mouunt
        res = client.AddPipe(
            CreateActor(3, 3),
            CreateActor(3, 3).NodeId(),
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            false);
        UNIT_ASSERT_C(!FAILED(res.Error.GetCode()), res.Error.GetMessage());
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

        auto info = CreateVolumeClientInfo(
            initialAccessMode,
            initialMountMode,
            0);

        TVolumeClientState client {info};

        NProto::TError ans;

        // when we have no pipes accessMode should be obtained from clientInfo
        ans = client.CheckLocalRequest(1, true, "", "");
        UNIT_ASSERT_C(FAILED(ans.GetCode()), "No Error returned");

        ans = client.CheckLocalRequest(1, false, "", "");
        UNIT_ASSERT_C(!FAILED(ans.GetCode()), "No Error returned");

        ans = client.CheckPipeRequest(CreateActor(1, 1), true, "", "");
        UNIT_ASSERT_C(FAILED(ans.GetCode()), "No Error returned");

        ans = client.CheckPipeRequest(CreateActor(1, 1), false, "", "");
        UNIT_ASSERT_C(!FAILED(ans.GetCode()), "No Error returned");

        // accessMode should be updated upon AddPipe
        auto res = client.AddPipe(
            CreateActor(1, 1),
            CreateActor(1, 1).NodeId(),
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            false);
        // changed -> return true
        UNIT_ASSERT_C(!FAILED(res.Error.GetCode()), res.Error.GetMessage());
        UNIT_ASSERT_VALUES_EQUAL(true, res.IsNew);
        UNIT_ASSERT_VALUES_EQUAL(1, client.GetPipes().size());

        ans = client.CheckLocalRequest(1, true, "", "");
        UNIT_ASSERT_C(!FAILED(ans.GetCode()), ans.GetMessage());

        ans = client.CheckLocalRequest(1, false, "", "");
        UNIT_ASSERT_C(!FAILED(ans.GetCode()), ans.GetMessage());

        res = client.AddPipe(
            CreateActor(2, 2),
            CreateActor(2, 2).NodeId(),
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE,
            false);

        UNIT_ASSERT_C(!FAILED(res.Error.GetCode()), res.Error.GetMessage());
        UNIT_ASSERT_VALUES_EQUAL(true, res.IsNew);
        UNIT_ASSERT_VALUES_EQUAL(2, client.GetPipes().size());

        ans = client.CheckPipeRequest(CreateActor(2, 2), true, "", "");
        UNIT_ASSERT_C(!FAILED(ans.GetCode()), ans.GetMessage());

        ans = client.CheckPipeRequest(CreateActor(2, 2), false, "", "");
        UNIT_ASSERT_C(!FAILED(ans.GetCode()), ans.GetMessage());

        ans = client.CheckLocalRequest(1, true, "", "");
        UNIT_ASSERT_C(FAILED(ans.GetCode()), "No Error returned");

        ans = client.CheckLocalRequest(1, false, "", "");
        UNIT_ASSERT_C(FAILED(ans.GetCode()), "No Error returned");

        // try to reactivate local pipe -> should fail
        res = client.AddPipe(
            CreateActor(1, 1),
            CreateActor(1, 1).NodeId(),
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            false);
        // changed -> return true
        UNIT_ASSERT_C(FAILED(res.Error.GetCode()), "No Error returned");

        res = client.AddPipe(
            CreateActor(3, 3),
            CreateActor(3, 3).NodeId(),
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            false);
        // changed -> return true
        UNIT_ASSERT_C(!FAILED(res.Error.GetCode()), res.Error.GetMessage());
        UNIT_ASSERT_VALUES_EQUAL(true, res.IsNew);
        UNIT_ASSERT_VALUES_EQUAL(3, client.GetPipes().size());

        ans = client.CheckLocalRequest(3, true, "", "");
        UNIT_ASSERT_C(!FAILED(ans.GetCode()), ans.GetMessage());

        ans = client.CheckLocalRequest(3, false, "", "");
        UNIT_ASSERT_C(!FAILED(ans.GetCode()), ans.GetMessage());

        ans = client.CheckPipeRequest(CreateActor(2, 2), true, "", "");
        UNIT_ASSERT_C(FAILED(ans.GetCode()), "No Error returned");

        ans = client.CheckPipeRequest(CreateActor(2, 2), false, "", "");
        UNIT_ASSERT_C(FAILED(ans.GetCode()), "No Error returned");
    }

    Y_UNIT_TEST(ShouldPreserveAccessModeOnPipeDisconnect)
    {
        auto initialMountMode = NProto::VOLUME_MOUNT_LOCAL;
        auto initialAccessMode = NProto::VOLUME_ACCESS_READ_WRITE;

        auto info = CreateVolumeClientInfo(
            initialAccessMode,
            initialMountMode,
            0);

        TVolumeClientState client {info};

        NProto::TError ans;

        ans = client.CheckLocalRequest(1, true, "", "");
        UNIT_ASSERT_C(!FAILED(ans.GetCode()), ans.GetMessage());

        client.AddPipe(
            CreateActor(3, 3),
            CreateActor(3, 3).NodeId(),
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            false);

        client.RemovePipe(CreateActor(3, 3), TInstant());

        ans = client.CheckLocalRequest(1, true, "", "");
        UNIT_ASSERT_C(!FAILED(ans.GetCode()), ans.GetMessage());
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
