#!/usr/bin/env python

from __future__ import print_function

import argparse
import contextlib
import logging
import os
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


QEMU_CONFIG = [
    '--static',
    '--prefix=/usr',
    '--extra-cflags=-O3 -fno-semantic-interposition -falign-functions=32 -D_FORTIFY_SOURCE=2 -fPIE',
    '--extra-ldflags=-z noexecstack -z relro -z now',
    '--target-list=x86_64-softmmu',
    '--audio-drv-list=',
    '--enable-attr',
    '--enable-cap-ng',
    # '--enable-curses',
    '--enable-kvm',
    '--enable-linux-aio',
    '--enable-numa',
    '--enable-tcg',
    '--enable-tools',
    '--enable-vhost-net',
    '--enable-virtfs',
    '--enable-vnc',
    '--enable-vnc-jpeg',
    '--enable-vnc-png',
    '--enable-vvfat',
    '--disable-brlapi',
    '--disable-bzip2',
    '--disable-curl',
    '--disable-debug-tcg',
    '--disable-docs',
    '--disable-fdt',
    '--disable-glusterfs',
    '--disable-gtk',
    '--disable-libiscsi',
    '--disable-libnfs',
    '--disable-libssh2',
    '--disable-libusb',
    '--disable-lzo',
    '--disable-opengl',
    '--disable-qom-cast-debug',
    '--disable-rdma',
    '--disable-sdl',
    '--disable-seccomp',
    '--disable-snappy',
    '--disable-spice',
    '--disable-tcg-interpreter',
    '--disable-tcmalloc',
    '--disable-tpm',
    '--disable-usb-redir',
    '--disable-vnc-sasl',
    '--disable-vte',
    '--disable-xen',
]


QEMU_DEPS = [
    'autoconf',
    'automake',
    'bc',
    'bison',
    'cpio',
    'flex',
    'gawk',
    'git',
    'libaio-dev',
    'libaudit-dev',
    'libcap-dev',
    'libcap-ng-dev',
    'libdw-dev',
    'libelf-dev',
    'libglib2.0-0',
    'libglib2.0-dev',
    'libjpeg-dev',
    'libltdl-dev',
    'libncurses5-dev',
    'libncursesw5-dev',
    'libnuma-dev',
    'libpixman-1-dev',
    'libpng-dev',
    'libtool',
    'ninja-build',
    'pkg-config',
    'pkg-config',
    'podlators-perl',
    'python',
    'python-dev',
    'python3-sphinx'
    'texinfo',
    'zlib1g-dev',
]


def run(args, **kwargs):
    print("+ '" + "' '".join(args) + "'")
    return subprocess.check_call(args, **kwargs)


def install_deps(args):
    run(['sudo', 'apt-get', 'install', '--no-install-recommends', '-y'] + QEMU_DEPS)


def preprocess(args):
    if args.src is None:
        args.src = os.path.abspath(
            os.path.join(os.getcwd(), 'qemu-' + args.git_tag if args.git_tag else 'src'))


def checkout(args):
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

        help = subprocess.check_output(
            [src_dir + '/configure', '--help'], cwd=build_dir)

        if isinstance(help, bytes):
            help = help.decode("utf-8")

        logging.debug(help)

        # commit b10d49d7619e4957b4b971f816661b57e5061d71
        if 'libssh2' not in help:
            QEMU_CONFIG.remove('--disable-libssh2')
            QEMU_CONFIG.append('--disable-libssh')

        run([src_dir + '/configure'] + QEMU_CONFIG, cwd=build_dir)
        run(['make', '-j', str(os.sysconf('SC_NPROCESSORS_ONLN'))], cwd=build_dir)

        if os.path.isdir(args.out):
            run(['make', 'install', 'DESTDIR=' +
                 os.path.abspath(args.out) +
                 '/qemu'], cwd=build_dir)
        else:
            with tmpdir(prefix='out-' + os.path.basename(args.src)) as out_dir:
                target_dir = os.path.abspath(out_dir) + '/qemu'
                target = os.path.abspath(args.out)

                run(['make', 'install', 'DESTDIR=' + target_dir], cwd=build_dir)
                with tarfile.open(target, 'w:gz') as tar:
                    for x in os.listdir(target_dir):
                        f = os.path.join(target_dir, x)
                        tar.add(f, arcname=os.path.relpath(f, target_dir))


def main(args):
    preprocess(args)

    if args.deps:
        install_deps(args)

    if args.co:
        checkout(args)

    build(args)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--src",
        help="qemu src directory or tarball",
        action="store")
    parser.add_argument(
        "--co",
        help="do checkout qemu source",
        action="store_true",
        default=False)
    parser.add_argument(
        "--git",
        help="source of the qemu",
        action="store",
        default="ssh://git@bb.yandex-team.ru/cloud/qemu.git")
    parser.add_argument(
        "--git-tag",
        help="specific tag",
        action="store",
        default="yc-5.0")
    parser.add_argument(
        "--deps",
        help="do install deps",
        action="store_true",
        default=False)
    parser.add_argument(
        "--out",
        help="target directory or tarball",
        action="store",
        default="qemu-static.tgz")

    args = parser.parse_args()
    main(args)
