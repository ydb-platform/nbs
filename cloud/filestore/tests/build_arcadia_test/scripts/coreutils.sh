set -ex

sudo apt update
sudo apt -y install python3 python-is-python3 git build-essential autoconf automake autopoint bison gperf libtool texinfo wget
cd $mountPath

if [ "$cloneOriginalRepo" == true ]; then
    # If cloneOriginalRepo is set to true, fetch stable version from original repo
    git clone --depth=1 --branch v9.3 --single-branch https://git.savannah.gnu.org/git/coreutils.git
    cd coreutils
    ./bootstrap
else
    # If cloneOriginalRepo is set to false, fetch uploaded version to sanbox (for isolation and reproducibility purposes)

    # Download latest published resource with matching attribute
    wget 'https://link/to/nbs_coreutils.tar' -O arch.tar
    tar xvf arch.tar
    cd coreutils
    git config --global --add safe.directory $$(pwd)
    sh -x bootstrap --no-git --gnulib-srcdir=../gnulib
fi
# Allow root user too run ./configure
export FORCE_UNSAFE_CONFIGURE=1
./configure
make check -j$$(nproc) | tee $coreutilsOutputPath
