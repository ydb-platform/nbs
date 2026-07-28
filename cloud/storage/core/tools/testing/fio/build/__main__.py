#!/usr/bin/env python

from __future__ import print_function

import argparse
import contextlib
import os
import platform
import re
import shutil
import subprocess
import tarfile
import tempfile


@contextlib.contextmanager
def tmpdir(**kwargs):
    tmp = tempfile.mkdtemp(**kwargs)
    try:
        yield tmp
    finally:
        shutil.rmtree(tmp)


X86_CFLAGS = [
    '-march=ivybridge',
    '-mno-bmi',
    '-mno-bmi2',
    '-mno-avx',
    '-mno-avx2',
]


FIO_CONFIG = [
    '--build-static',
    '--prefix=/usr',
]


FIO_DEPS = [
    'git',
]

DEFAULT_OUT_BASENAME = 'fio-static'


def run(args, **kwargs):
    print("+ '" + "' '".join(args) + "'")
    return subprocess.check_call(args, **kwargs)


def fio_cflags():
    machine = platform.machine().lower()
    if machine in ('x86_64', 'amd64'):
        return X86_CFLAGS

    return []


def fio_config():
    config = list(FIO_CONFIG)
    cflags = fio_cflags()
    if cflags:
        config.append('--extra-cflags=' + ' '.join(cflags))

    return config


def default_out_path(fio_version):
    version = re.sub(r'[^A-Za-z0-9._-]+', '_', fio_version or 'unknown')
    machine = platform.machine().lower()
    return "{}-{}-{}.tgz".format(DEFAULT_OUT_BASENAME, version, machine)


def install_deps(args):
    run(['sudo', 'apt-get', 'install', '--no-install-recommends', '-y'] + FIO_DEPS)


def checkout(args):
    if args.src is None:
        args.src = os.path.abspath(
            os.path.join(os.getcwd(), 'fio-' + args.git_tag if args.git_tag else 'src'))

    if not os.path.exists(args.src):
        os.mkdir(args.src)
    else:
        raise RuntimeError("src path already exists {}".format(args.src))

    run(['git', 'clone', args.git, args.src])
    if args.git_tag is not None:
        run(['git', 'checkout', args.git_tag], cwd=args.src)


def build(args):
    with tmpdir(prefix='build-' + os.path.basename(args.src)) as build_dir:
        if os.path.isdir(args.src):
            src_dir = os.path.abspath(args.src)
        else:
            run(['tar', '--strip-components=1', '-xf',
                 os.path.abspath(args.src)], cwd=build_dir)
            src_dir = build_dir

        if args.out is None:
            args.out = default_out_path(args.git_tag)

        run([src_dir + '/configure'] + fio_config(), cwd=build_dir)
        run(['make', '-j', str(os.sysconf('SC_NPROCESSORS_ONLN'))], cwd=build_dir)

        if os.path.isdir(args.out):
            run(['make', 'install', 'DESTDIR=' +
                 os.path.abspath(args.out) +
                 '/fio'], cwd=build_dir)
        else:
            with tmpdir(prefix='out-' + os.path.basename(args.src)) as out_dir:
                target_dir = os.path.abspath(out_dir) + '/fio'
                target = os.path.abspath(args.out)

                run(['make', 'install', 'DESTDIR=' + target_dir], cwd=build_dir)
                with tarfile.open(target, 'w:gz') as tar:
                    for x in os.listdir(target_dir):
                        f = os.path.join(target_dir, x)
                        tar.add(f, arcname=os.path.relpath(f, target_dir))


def main(args):
    if args.deps:
        install_deps(args)

    if args.co:
        checkout(args)

    build(args)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--src",
        help="fio src directory or tarball; required unless --co is used",
        action="store")
    parser.add_argument(
        "--co",
        help="do checkout fio source",
        action="store_true",
        default=False)
    parser.add_argument(
        "--git",
        help="source of the fio",
        action="store",
        default="https://github.com/axboe/fio")
    parser.add_argument(
        "--git-tag",
        help="specific tag",
        action="store",
        default=None)
    parser.add_argument(
        "--deps",
        help="do install deps",
        action="store_true",
        default=False)
    parser.add_argument(
        "--out",
        help="target directory or tarball; defaults to fio-static-<version>-<arch>.tgz",
        action="store",
        default=None)

    args = parser.parse_args()
    if args.src is None and not args.co:
        parser.error("--src is required unless --co is used")
    main(args)
