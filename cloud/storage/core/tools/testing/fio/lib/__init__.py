import itertools
import json
import logging
import os
import random
import subprocess
import sys
import uuid

import yatest.common as common

logger = logging.getLogger(__name__)

KB = 1024
MB = 1024 * 1024


def _print_size(size):
    if size < KB:
        return str(size)
    elif size < MB:
        return "{}K".format(int(size / KB))
    else:
        return "{}M".format(int(size / MB))


class TestCase:
    def __init__(self, scenario, size, request_size, iodepth, sync, duration, compress_percentage,
                 verify, unlink, numjobs, fsync, fdatasync, end_fsync, write_barrier, unique_name,
                 offset, randseed):
        self._scenario = scenario
        self._size = size
        self._request_size = request_size
        self._iodepth = iodepth
        self._sync = sync
        self._duration = duration
        self._compress_percentage = compress_percentage
        self._verify = verify
        self._unlink = unlink
        self._numjobs = numjobs
        self._fsync = fsync
        self._fdatasync = fdatasync
        self._end_fsync = end_fsync
        self._write_barrier = write_barrier
        self._unique_name = unique_name
        self._offset = offset
        self._randseed = randseed

    @property
    def scenario(self):
        return self._scenario

    @property
    def size(self):
        return self._size

    @property
    def request_size(self):
        return self._request_size

    @property
    def iodepth(self):
        return self._iodepth

    @property
    def sync(self):
        return self._sync

    @property
    def duration(self):
        return self._duration

    @property
    def compress_percentage(self):
        return self._compress_percentage

    @property
    def verify(self):
        return self._verify

    @property
    def unlink(self):
        return self._unlink

    @property
    def numjobs(self):
        return self._numjobs

    @property
    def fsync(self):
        return self._fsync

    @property
    def fdatasync(self):
        return self._fdatasync

    @property
    def end_fsync(self):
        return self._end_fsync

    @property
    def write_barrier(self):
        return self._write_barrier

    @property
    def unique_name(self):
        return self._unique_name

    @property
    def offset(self):
        return self._offset

    @property
    def randseed(self):
        return self._randseed

    @property
    def name(self):
        parts = [
            self.scenario,
            _print_size(self.request_size),
            str(self.iodepth)
        ]
        if self.sync:
            parts += ["sync"]
        if self.numjobs > 1:
            parts += ['jobs', str(self.numjobs)]
        if self.fsync > 0:
            parts += ['fsync', str(self.fsync)]
        if self.fdatasync > 0:
            parts += ['fdatasync', str(self.fdatasync)]
        if self.write_barrier > 0:
            parts += ['write_barrier', str(self.write_barrier)]
        if self.unlink:
            parts += ['unlink']
        if self.end_fsync:
            parts += ['end-fsync']
        name = "_".join(parts)
        if self.unique_name:
            name += "_" + str(uuid.uuid4())
        return name

    def get_common_fio_cmd(self, fio_bin):
        cmd = [
            fio_bin,
            "--name", self.name,
            "--rw", str(self.scenario),
            "--size", str(self.size),
            "--bs", str(self.request_size),
            "--buffer_compress_percentage", str(self.compress_percentage),
            "--runtime", str(self.duration),
            "--time_based",
            "--output-format", "json",
        ]
        if self.offset:
            cmd += ["--offset", str(self.offset)]
        if self.verify and 'read' not in self.scenario:
            cmd += [
                "--verify", "md5",
                "--verify_backlog", "8192",  # 32MiB
                # "--verify_fatal", "1",
                "--serialize_overlap", "1",
                "--verify_dump", "1",

            ]
        if self.sync:
            cmd += [
                "--buffered", "1",
                "--ioengine", "sync",
                "--numjobs", str(self.iodepth),
            ]
        else:
            cmd += [
                "--direct", "1",
                "--ioengine", "libaio",
                "--iodepth", str(self.iodepth),
            ]

        if self.randseed:
            cmd += ["--randseed", str(self.randseed)]
        else:
            cmd += ["--randseed", str(random.randint(1, sys.maxsize))]
        return cmd

    def get_fio_cmd(self, fio_bin, file_name):
        cmd = self.get_common_fio_cmd(fio_bin)
        cmd += ["--filename", file_name]
        return cmd

    def get_index_fio_cmd(self, fio_bin, directory):
        cmd = self.get_common_fio_cmd(fio_bin)
        cmd += [
            "--directory", directory,
            "--numjobs", str(self.numjobs)
        ]
        if self.fsync > 0:
            cmd += ["--fsync", str(self.fsync)]
        if self.fdatasync > 0:
            cmd += ["--fdatasync", str(self.fdatasync)]
        if self.write_barrier > 0:
            cmd += ["--write_barrier", str(self.write_barrier)]
        if self.unlink:
            cmd += ["--unlink", "1"]
        if self.end_fsync:
            cmd += ["--end_fsync", "1"]
        return cmd


def _generate_tests(size, duration, sync, scenarios, sizes, iodepths, compress_percentage, verify,
                    unlinks, numjobs, fsyncs, fdatasyncs, end_fsyncs, write_barriers, unique_name, offset, randseed):
    return [
        TestCase(scenario, size, request_size, iodepth, sync, duration, compress_percentage, verify,
                 unlink, numjob, fsync, fdatasync, end_fsync, write_barrier, unique_name,
                 offset, randseed)
        for scenario, request_size, iodepth, unlink, numjob, fsync, fdatasync, end_fsync, write_barrier
        in itertools.product(scenarios, sizes, iodepths, unlinks, numjobs, fsyncs, fdatasyncs,
                             end_fsyncs, write_barriers)
    ]


def generate_tests(size=100 * MB, duration=60, sync=False, scenarios=['randread', 'randwrite', 'randrw'],
                   sizes=[4 * KB, 4 * MB], iodepths=[1, 32], compress_percentage=90, verify=True, unlinks=[False],
                   numjobs=[1], fsyncs=[0], fdatasyncs=[0], end_fsyncs=[False], write_barriers=[0],
                   unique_name=False, offset=0, randseed=None):
    return {
        test.name: test
        for test in _generate_tests(size, duration, sync, scenarios, sizes, iodepths, compress_percentage,
                                    verify, unlinks, numjobs, fsyncs, fdatasyncs, end_fsyncs, write_barriers,
                                    unique_name, offset, randseed)
    }


def generate_default_test():
    return generate_tests(size=100 * MB, duration=60, sync=False, scenarios=['randrw'],
                          sizes=[4 * KB], iodepths=[32], compress_percentage=90, verify=True, unlinks=[False],
                          numjobs=[1], fsyncs=[0], fdatasyncs=[0], end_fsyncs=[False], write_barriers=[0],
                          unique_name=False)


def generate_index_tests(duration=30, scenarios=['randrw'], sizes=[4 * KB], iodepths=[16], unlinks=[False, True],
                         numjobs=[1, 4], fsyncs=[0, 16], fdatasyncs=[0, 8], end_fsyncs=[False, True],
                         write_barriers=[0, 4], unique_name=False):
    return generate_tests(duration=duration, scenarios=scenarios, sizes=sizes, iodepths=iodepths, unlinks=unlinks,
                          numjobs=numjobs, fsyncs=fsyncs, fdatasyncs=fdatasyncs, end_fsyncs=end_fsyncs,
                          write_barriers=write_barriers, unique_name=unique_name)


def generate_default_index_test():
    return generate_index_tests(duration=30, scenarios=['randrw'], sizes=[4 * KB], iodepths=[16], unlinks=[True],
                                numjobs=[4], fsyncs=[16], fdatasyncs=[8], end_fsyncs=[True], write_barriers=[4],
                                unique_name=False)


def _get_fio_bin():
    fio_bin = common.build_path(
        "cloud/storage/core/tools/testing/fio/bin/fio")
    if not os.path.exists(fio_bin):
        raise Exception("cannot find fio binary at path " + fio_bin)
    return fio_bin


def get_file_name(mount_dir, test_name):
    if not os.path.exists(mount_dir):
        raise Exception("invalid path " + mount_dir)
    return "{}/{}.dat".format(mount_dir, test_name)


def get_dir_name(mount_dir, test_name):
    if not os.path.exists(mount_dir):
        raise Exception("invalid path " + mount_dir)
    return "{}/{}".format(mount_dir, test_name)


def _lay_out_file(file_name, size):
    # do not touch pre-existing files (e.g. device nodes)
    try:
        fd = os.open(file_name, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o644)
    except OSError:
        return

    try:
        os.truncate(fd, size)
    finally:
        os.close(fd)


def _lay_out_files(directory, name, jobs, size):
    if not os.path.exists(directory):
        os.makedirs(directory)

    for i in range(jobs):
        _lay_out_file('{}/{}.{}.0'.format(directory, name, i), size)


def _execute_command(cmd, fail_on_errors):
    logger.info("execute " + " ".join(cmd))
    ex = common.execute(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)

    results = json.loads(ex.stdout)

    # TODO: canonize something useful
    errors = 0
    for job in results["jobs"]:
        errors += int(job["error"])
    if fail_on_errors:
        assert errors == 0
    return "errors: " + str(errors)


def run_test(file_name, test, fail_on_errors=False):
    # fio lays out the test file using the job blocksize, which may exhaust the
    # run time limit, so do it ourselves
    logger.info("laying out file " + file_name)
    _lay_out_file(file_name, test.size)
    logger.info("laid out")

    fio_bin = _get_fio_bin()
    cmd = test.get_fio_cmd(fio_bin, file_name)

    return _execute_command(cmd, fail_on_errors)


def run_index_test(directory, test, fail_on_errors=False):
    # fio lays out the test file using the job blocksize, which may exhaust the
    # run time limit, so do it ourselves
    logger.info("laying out files in directory " + directory)
    _lay_out_files(directory, test.name, test.numjobs, test.size)
    logger.info("laid out")

    fio_bin = _get_fio_bin()
    cmd = test.get_index_fio_cmd(fio_bin, directory)

    return _execute_command(cmd, fail_on_errors)
