set -ex

# hack until CLOUD-135098 or CLOUD-132193 is solved
echo "2a02:6b8::183 mirror.yandex.ru" >> /etc/hosts
echo "2604:1380:4601:e00::1 git.kernel.org" >> /etc/hosts
systemctl restart systemd-hostnamed

# install packages
sudo apt update
sudo apt -y upgrade
sudo apt -y install git acl attr automake bc dbench dump e2fsprogs fio gawk gcc git indent libacl1-dev libaio-dev libcap-dev libgdbm-dev libtool libtool-bin libuuid1 lvm2 make psmisc python3 quota sed uuid-dev uuid-runtime xfsprogs linux-headers-`uname -r` sqlite3 f2fs-tools ocfs2-tools udftools xfsdump xfslibs-dev liburing-dev exfatprogs

# clone repository and build project
git clone git://git.kernel.org/pub/scm/fs/xfs/xfstests-dev.git
cd xfstests-dev
make
sudo make install

# setup building layout
mkdir $testPath
mkdir $scratchPath
sudo mount -t virtiofs $testDevice $testPath
sudo mount -t virtiofs $scratchDevice $scratchPath

#setup local.config
echo "export TEST_DEV=$testDevice" >> local.config
echo "export TEST_DIR=$testPath" >> local.config
echo "export SCRATCH_DEV=$scratchDevice" >> local.config
echo "export SCRATCH_MNT=$scratchPath" >> local.config
echo "export FSTYP=$fsType" >> local.config

#create test user
sudo useradd -m fsgqa
sudo useradd 123456-fsgqa
sudo useradd fsgqa2

#run suite
./check 2>&1 'generic/1??' | tee ~/logs.txt
