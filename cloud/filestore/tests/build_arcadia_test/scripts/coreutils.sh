set -ex

sudo apt update
sudo apt -y install python3 python-is-python3 git build-essential autoconf automake autopoint bison gperf libtool texinfo wget
cd $mountPath

if [ ! -d "coreutils" ]; then
    git clone --depth=1 --branch v9.3 --single-branch https://git.savannah.gnu.org/git/coreutils.git
    cd coreutils
    ./bootstrap
    # Allow root user too run ./configure
    export FORCE_UNSAFE_CONFIGURE=1
    ./configure
else
    cd coreutils
fi

make check -j$$(nproc) | tee $coreutilsOutputPath
