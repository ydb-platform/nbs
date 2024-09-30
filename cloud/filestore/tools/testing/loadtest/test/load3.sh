
# strace --follow-forks -- git clone --depth 1 --recursive https://github.com/google/leveldb 2>&1 | tee strace.log
git clone --depth 1 --recursive https://github.com/google/leveldb
cd leveldb
cmake -G Ninja .
ninja
ctest
