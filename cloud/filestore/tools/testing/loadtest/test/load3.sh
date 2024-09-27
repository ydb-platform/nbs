
git clone --depth 1 --recursive https://github.com/google/leveldb
cd leveldb
cmake -G Ninja .
ninja
ctest
