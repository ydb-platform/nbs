#!/bin/sh
# Test ls -q option - show non-printable characters as ?

desc="ls -q replaces non-printable characters with ?"

dir=`dirname $0`
. ${dir}/../misc.sh

echo "1..17"

# ls -q shows normal filenames unchanged
expect 0 create normalfile 0644
output=`ls -q normalfile 2>&1`
test_check "$output" = "normalfile"
expect 0 unlink normalfile

# ls -q replaces tab in filename with ?
filename="file$(printf '\t')name"
expect 0 create "$filename" 0644
output=`ls -q 2>&1 | grep "file?name" | wc -l`
test_check "$output" -eq 1
expect 0 unlink "$filename"

# ls -q replaces control characters with ?
filename="file$(printf '\007')bell"
expect 0 create "$filename" 0644
output=`ls -q 2>&1`
test_check "$output" = "file?bell"
expect 0 unlink "$filename"

# ls -q with multiple files containing special chars
filename1="file$(printf '\006')1"
filename2="file$(printf '\007')2"
expect 0 create "$filename1" 0644
expect 0 create "$filename2" 0644
output=`ls -q 2>&1 | tr '\n' ' ' | sed 's/ $//'`
test_check_quoted "$output" = "file?1 file?2"
expect 0 unlink "$filename1"
expect 0 unlink "$filename2"

# ls -q works with other options
filename="bad$(printf '\007')name"
expect 0 create "$filename" 0644
output=`ls -lq 2>&1 | grep "bad?name" | wc -l`
test_check "$output" -eq 1
expect 0 unlink "$filename"
