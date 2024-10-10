#include "utils.h"

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/folder/tempdir.h>
#include <util/stream/file.h>

namespace NCloud::NBlockStore::NBD {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TUtilsTest)
{
    Y_UNIT_TEST(ShouldFindMountedFiles)
    {
        auto mountInfoStr = R"(
            356 355 0:49 / /proc rw,nosuid,nodev,noexec,relatime shared:146 - proc proc rw
            357 355 0:50 / /dev rw,nosuid shared:147 - tmpfs tmpfs rw,size=65536k,mode=755,inode64
            358 357 0:51 / /dev/pts rw,nosuid,noexec,relatime shared:155 - devpts devpts rw,gid=5,mode=620,ptmxmode=666
            359 355 0:52 / /sys ro,nosuid,nodev,noexec,relatime shared:176 - sysfs sysfs ro
            360 359 0:29 / /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime shared:203 - cgroup2 cgroup rw,nsdelegate,memory_recursiveprot
            361 357 0:48 / /dev/mqueue rw,nosuid,nodev,noexec,relatime shared:158 - mqueue mqueue rw
            362 357 0:53 / /dev/shm rw,nosuid,nodev,noexec,relatime shared:172 - tmpfs shm rw,size=65536k,inode64
            363 355 252:2 /var/lib/docker/volumes/minikube/_data /var rw,relatime shared:205 master:1 - ext4 /dev/vda2 rw
            364 355 0:54 / /run rw,nosuid,nodev,noexec,relatime shared:206 - tmpfs tmpfs rw,inode64
            366 355 0:56 / /tmp rw,nosuid,nodev,noexec,relatime shared:207 - tmpfs tmpfs rw,inode64
            368 355 252:2 /usr/lib/modules /usr/lib/modules ro,relatime shared:234 - ext4 /dev/vda2 rw
            173 357 0:51 /0 /dev/console rw,nosuid,noexec,relatime shared:175 - devpts devpts rw,gid=5,mode=620,ptmxmode=666
            200 364 0:57 / /run/lock rw,nosuid,nodev,noexec,relatime shared:282 - tmpfs tmpfs rw,size=5120k,inode64
            211 357 0:59 / /dev/hugepages rw,relatime shared:284 - hugetlbfs hugetlbfs rw,pagesize=2M
            224 359 0:7 / /sys/kernel/debug rw,nosuid,nodev,noexec,relatime shared:285 - debugfs debugfs rw
            227 359 0:12 / /sys/kernel/tracing rw,nosuid,nodev,noexec,relatime shared:288 - tracefs tracefs rw
            228 359 0:34 / /sys/fs/fuse/connections rw,nosuid,nodev,noexec,relatime shared:289 - fusectl fusectl rw
            511 364 0:60 / /run/credentials/systemd-sysusers.service ro,nosuid,nodev,noexec,relatime shared:290 - ramfs none rw,mode=700
            229 209 0:36 / /proc/sys/fs/binfmt_misc rw,nosuid,nodev,noexec,relatime shared:291 - binfmt_misc binfmt_misc rw
            236 355 252:2 /var/lib/docker/volumes/minikube/_data/data /data rw,relatime shared:205 master:1 - ext4 /dev/vda2 rw
            244 366 252:2 /var/lib/docker/volumes/minikube/_data/hostpath_pv /tmp/hostpath_pv rw,relatime shared:205 master:1 - ext4 /dev/vda2 rw
            252 366 252:2 /var/lib/docker/volumes/minikube/_data/hostpath-provisioner /tmp/hostpath-provisioner rw,relatime shared:205 master:1 - ext4 /dev/vda2 rw
            272 364 0:4 net:[4026533085] /run/docker/netns/default rw shared:293 - nsfs nsfs rw
            854 364 0:4 net:[4026533185] /run/docker/netns/d8ef02b88399 rw shared:316 - nsfs nsfs rw
            959 363 0:182 / /var/lib/docker/containers/blabla1/mounts/shm rw,nosuid,nodev,noexec,relatime shared:321 - tmpfs shm rw,size=65536k,inode64
            932 363 0:180 / /var/lib/docker/containers/blabla2/mounts/shm rw,nosuid,nodev,noexec,relatime shared:322 - tmpfs shm rw,size=65536k,inode64
            826 363 0:139 / /var/lib/kubelet/pods/5f8a1a7e-d9f0-48be-97d3-8d655855d5fc/volumes/kubernetes.io~projected/kube-api-access-mgmsb rw,relatime shared:308 - tmpfs tmpfs rw,size=393216k,inode64
            831 363 0:143 / /var/lib/kubelet/pods/05108687-5f0d-42c7-890d-bd2dd69d6466/volumes/kubernetes.io~projected/kube-api-access-lz87t rw,relatime shared:309 - tmpfs tmpfs rw,size=262144k,inode64
            847 363 0:144 / /var/lib/docker/containers/blabla3/mounts/shm rw,nosuid,nodev,noexec,relatime shared:312 - tmpfs shm rw,size=65536k,inode64
            869 363 0:163 / /var/lib/docker/containers/blabla4/mounts/shm rw,nosuid,nodev,noexec,relatime shared:323 - tmpfs shm rw,size=65536k,inode64
            894 363 0:177 / /var/lib/kubelet/pods/blabla5/volumes/kubernetes.io~projected/kube-api-access-wf5ls rw,relatime shared:317 - tmpfs tmpfs rw,size=165029212k,inode64
            1008 363 0:243 /nbd1 /var/lib/kubelet/plugins/kubernetes.io/csi/volumeDevices/publish/pvc-blabla6/blabla5 rw,nosuid shared:335 - tmpfs tmpfs rw,size=65536k,mode=755,inode64
            1009 363 0:243 /nbd1 /var/lib/kubelet/plugins/kubernetes.io/csi/volumeDevices/pvc-blabla6/dev/blabla5 rw shared:335 - tmpfs tmpfs rw,size=65536k,mode=755,inode64
            1015 363 0:253 / /var/lib/docker/containers/blabla7/mounts/shm rw,nosuid,nodev,noexec,relatime shared:337 - tmpfs shm rw,size=65536k,inode64
            1428 364 0:4 net:[4026533292] /run/docker/netns/5dc95378b825 rw shared:338 - nsfs nsfs rw
        )";

        TTempDir dir;
        auto mountInfoPath = dir.Path() / "mountinfo";
        TOFStream(mountInfoPath.GetPath()).Write(mountInfoStr);

        auto mountedFiles = FindMountedFiles(
            "/dev/nbd1",
            mountInfoPath.GetPath());

        TSet<TString> expectedFiles = {
            "/dev/nbd1",
            "/var/lib/kubelet/plugins/kubernetes.io/csi/volumeDevices/publish/pvc-blabla6/blabla5",
            "/var/lib/kubelet/plugins/kubernetes.io/csi/volumeDevices/pvc-blabla6/dev/blabla5",
        };
        UNIT_ASSERT_VALUES_EQUAL(expectedFiles, mountedFiles);
    }

    Y_UNIT_TEST(ShouldFindLoopbackDevices)
    {
        TTempDir sysBlockDir;

        TSet<TString> mountedFiles = {
            "/dev/nbd1",
            "/var/lib/kubelet/plugins/kubernetes.io/csi/volumeDevices/publish/pvc-blabla6/blabla5",
            "/var/lib/kubelet/plugins/kubernetes.io/csi/volumeDevices/pvc-blabla6/dev/blabla5",
        };

        TFsPath backingFile;
        backingFile = sysBlockDir.Path() / "loop0" / "loop" / "backing_file";
        backingFile.Parent().MkDirs();
        TOFStream(backingFile.GetPath()).Write(R"(
            /var/lib/snapd/snaps/lxd_27428.snap
            )");

        backingFile = sysBlockDir.Path() / "loop1" / "loop" / "backing_file";
        backingFile.Parent().MkDirs();
        TOFStream(backingFile.GetPath()).Write(R"(
            /var/lib/snapd/snaps/core20_2105.snap
            )");

        backingFile = sysBlockDir.Path() / "loop2" / "loop" / "backing_file";
        backingFile.Parent().MkDirs();
        TOFStream(backingFile.GetPath()).Write(R"(
            /
            )");

        backingFile = sysBlockDir.Path() / "loop3" / "loop" / "backing_file";
        backingFile.Parent().MkDirs();
        TOFStream(backingFile.GetPath()).Write(R"(
            /var/lib/kubelet/plugins/kubernetes.io/csi/volumeDevices/pvc-blabla6/dev/blabla5
            )");

        backingFile = sysBlockDir.Path() / "loop4" / "loop" / "backing_file";
        backingFile.Parent().MkDirs();
        TOFStream(backingFile.GetPath()).Write(R"(
            /var/lib/snapd/snaps/core20_2182.snap
            )");

        auto loopbackDevices = FindLoopbackDevices(
            mountedFiles,
            sysBlockDir.Path());

        TVector<TString> expectedDevices = {"/dev/loop3"};
        UNIT_ASSERT_VALUES_EQUAL(expectedDevices, loopbackDevices);
    }

    Y_UNIT_TEST(ShouldFindFreeNbdDevice)
    {
        TTempDir sysBlockDir;
        TFsPath path = sysBlockDir.Path();

        auto createPidFile = [](auto path) {
            path.Parent().MkDir();
            TOFStream(path.GetPath()).Write("42");
        };

        (path / "sda0").MkDir();
        createPidFile(path / "sda1" / "pid");
        createPidFile(path / "nbd0" / "pid");
        createPidFile(path / "nbd1" / "pid");

        UNIT_ASSERT(FindFreeNbdDevice(path.GetPath()).empty());

        (path / "nbd2").MkDir();

        UNIT_ASSERT_VALUES_EQUAL(
            FindFreeNbdDevice(path.GetPath()),
            TString("/dev/nbd2"));
    }
}

}   // namespace NCloud::NBlockStore::NBD
