#!/bin/bash

# It is not part of official posix compliance suite. Inspired by util-linux.

echo "1..1"

die() {
    echo "$@"
    exit 1
}

do_lock () {
	local opts="$1"
	local expected_rc="$2"
	local mesg="$3"

	$FLOCK_TOOL $1 $WORKING_DIR/lockfile echo "$msg" 2>&1

    local rc="$?"
	if [  "$rc" == "$expected_rc" ]; then
        echo "success $1"
	else
        echo "failed $1 $mesg [rc=$rc]"
	fi
}

[ -f $FLOCK_TOOL ] || die "no flock available"
[ -f .release_lock ] && die "release flag already exist"

WORKING_DIR="flock"
mkdir $WORKING_DIR || die "$WORKING_DIR already exists"

# general lock
echo "Locking"
$FLOCK_TOOL --shared $WORKING_DIR/lockfile \
	bash -c 'while [ ! -f .release_lock ]; do sleep 1; done; echo "Unlocking"' 2>&1 &

pid=$!
# check for running background process
if [ "$pid" -le "0" ] || ! kill -s 0 "$pid" &>/dev/null; then
	die "unable to run flock"
fi

# the lock should be established when flock has a child
timeout 15s bash -c "while ! pgrep -P $pid >/dev/null; do sleep 0.1 ;done" \
	|| die "timeout waiting for flock child"

# fail to lock nonblock
do_lock "--nonblock --conflict-exit-code 123" 123 "non-block exclusive lock didn't fail as expected"

do_lock "--no-fork --nonblock --conflict-exit-code 123" 123 "non-block no-fork lock didn't fail"

do_lock "--shared --timeout 1 --conflict-exit-code 123" 0 "couldn't obtain shared lock"

# this is the same as non-block test (exclusive lock is the default), but here
# we explicitly specify --exclusive on command line
do_lock "--nonblock --exclusive --conflict-exit-code 123" 123 "explicit exclusive lock failed"

# check for running background process
if [ "$pid" -le "0" ] || ! kill -s 0 "$pid" &>/dev/null; then
	die "background process has failed"
fi

# allow background lock exit
bash -c 'sleep 1; echo "setting release lock flag"; touch .release_lock' &

# sync wait for to acquire lock
do_lock "--timeout 10 --conflict-exit-code 123" 0 "failed to obtain lock within timeout"

rm -rf $WORKING_DIR
