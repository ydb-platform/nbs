#!/bin/bash
# Test ls sorting options - -t, -S, -r, -c, -u

desc="ls sorting options work correctly"

dir=`dirname $0`
. ${dir}/../misc.sh

echo "1..50"

n0=`namegen`
n1=`namegen`
n2=`namegen`

# ls -t sorts by modification time (newest first) TODO uncomment after fix flop on qemu
# expect 0 create file1 0644
# sleep 0.1
# expect 0 create file2 0644
# sleep 0.1
# expect 0 create file3 0644
# output=`ls -t 2>&1 | tr '\n' ' ' | sed 's/ $//'`
# test_check_quoted "$output" = "file3 file2 file1"
# expect 0 unlink file1
# expect 0 unlink file2
# expect 0 unlink file3

# ls -tr reverses time sort (oldest first)
expect 0 create file1 0644
sleep 0.1
expect 0 create file2 0644
sleep 0.1
expect 0 create file3 0644
output=`ls -tr 2>&1 | tr '\n' ' ' | sed 's/ $//'`
test_check_quoted "$output" = "file1 file2 file3"
expect 0 unlink file1
expect 0 unlink file2
expect 0 unlink file3

# ls -S sorts by file size (largest first)
expect 0 create small 0644
echo "x" > small
expect 0 create medium 0644
echo "xxxxxxxxxx" > medium
expect 0 create large 0644
echo "xxxxxxxxxxxxxxxxxxxx" > large
output=`ls -S 2>&1 | tr '\n' ' ' | sed 's/ $//'`
test_check_quoted "$output" = "large medium small"
expect 0 unlink small
expect 0 unlink medium
expect 0 unlink large

# ls -Sr reverses size sort (smallest first)
expect 0 create small 0644
echo "x" > small
expect 0 create medium 0644
echo "xxxxxxxxxx" > medium
expect 0 create large 0644
echo "xxxxxxxxxxxxxxxxxxxx" > large
output=`ls -Sr 2>&1 | tr '\n' ' ' | sed 's/ $//'`
test_check_quoted "$output" = "small medium large"
expect 0 unlink small
expect 0 unlink medium
expect 0 unlink large

# ls -r reverses alphabetical sort
expect 0 create aaa 0644
expect 0 create bbb 0644
expect 0 create ccc 0644
output=`ls -r 2>&1 | tr '\n' ' ' | sed 's/ $//'`
test_check_quoted "$output" = "ccc bbb aaa"
expect 0 unlink aaa
expect 0 unlink bbb
expect 0 unlink ccc

# ls -c uses status change time for sorting
expect 0 create file1 0644
sleep 0.1
expect 0 create file2 0644
sleep 0.1
expect 0 chmod file1 0755
output=`ls -tc 2>&1 | head -1`
test_check "$output" = "file1"
expect 0 unlink file1
expect 0 unlink file2

# ls -u uses access time for sorting TODO uncomment after fix flop on qemu
# expect 0 create file1 0644
# expect 0 create file2 0644
# sleep 0.1
# # Access file2 to update its access time
# cat file2 > /dev/null 2>&1
# output=`ls -tu 2>&1 | head -1`
# test_check "$output" = "file2"
# expect 0 unlink file1
# expect 0 unlink file2

# ls -cr combines status change time with reverse
expect 0 create file1 0644
sleep 0.1
expect 0 create file2 0644
sleep 0.1
expect 0 chmod file1 0755
output=`ls -tcr 2>&1 | head -1`
test_check "$output" = "file2"
expect 0 unlink file1
expect 0 unlink file2

# ls -ur combines access time with reverse TODO uncomment after fix flop on qemu
# expect 0 create file1 0644
# expect 0 create file2 0644
# sleep 0.1
# cat file2 > /dev/null 2>&1
# output=`ls -tur 2>&1 | head -1`
# test_check "$output" = "file1"
# expect 0 unlink file1
# expect 0 unlink file2

# ls -t with same modification time falls back to name
expect 0 create aaa 0644
expect 0 create zzz 0644
touch -r aaa zzz
output=`ls -t aaa zzz 2>&1 | head -1`
test_check "$output" = "aaa"
expect 0 unlink aaa
expect 0 unlink zzz

# ls -S with same size falls back to name
expect 0 create aaa 0644
expect 0 create zzz 0644
output=`ls -S aaa zzz 2>&1 | head -1`
test_check "$output" = "aaa"
expect 0 unlink aaa
expect 0 unlink zzz

# ls -At sorting works with hidden files TODO uncomment after fix flop on qemu
# expect 0 create .hidden1 0644
# sleep 0.1
# expect 0 create .hidden2 0644
# output=`ls -At 2>&1 | head -1`
# test_check "$output" = ".hidden2"
# expect 0 unlink .hidden1
# expect 0 unlink .hidden2

#ls -dt sorting works with directories TODO uncomment after fix flop on qemu
# expect 0 mkdir dir1 0755
# sleep 0.1
# expect 0 mkdir dir2 0755
# output=`ls -dt dir[12] 2>&1 | head -1`
# test_check "$output" = "dir2"
# expect 0 rmdir dir1
# expect 0 rmdir dir2
