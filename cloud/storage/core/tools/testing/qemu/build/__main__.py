#!/usr/bin/env python

from __future__ import print_function

import argparse
import contextlib
import logging
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

QEMU_CONFIG = [
    '--static',
    '--prefix=/usr',
    '--extra-cflags=-O3 -fno-semantic-interposition -falign-functions=32 -D_FORTIFY_SOURCE=2 -fPIE -Wno-maybe-uninitialized -Wno-array-bounds -Wno-deprecated-declarations -Wno-stringop-overflow',
    '--extra-ldflags=-z noexecstack -z relro -z now',
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
    '--enable-vvfat',
    '--disable-auth-pam',
    '--disable-brlapi',
    '--disable-bzip2',
    '--disable-curl',
    '--disable-debug-tcg',
    '--disable-docs',
    '--disable-glusterfs',
    '--disable-gtk',
    '--disable-libiscsi',
    '--disable-libnfs',
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
    #'--disable-tpm',
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
    'meson',
    'ninja-build',
    'pkg-config',
    'pkg-config',
    'podlators-perl',
    'python3',
    'python3-tomli',
    'texinfo',
    'zlib1g-dev',
]


QEMU_CONFIG_MIN_VERSION = [
    ('--disable-gio', (7, 0, 0)),
    ('--enable-slirp', (7, 0, 0)),
]

QEMU_CONFIG_MAX_VERSION = [
    ('--enable-vnc-png', (7, 0, 0)),
    ('--disable-tcmalloc', (6, 0, 0)),
]

LIBSLIRP_BUILD_SCRIPT = 'build-libslirp-static.sh'
LIBSLIRP_DEFAULT_REF = 'v4.9.3'

GIT_TAG_VERSION_RE = re.compile(
    r'(?:^|[^0-9])(\d+)\.(\d+)(?:\.(\d+))?(?:[-.]?rc(\d+))?(?=$|[^0-9])')


def git_tag_version_key(git_tag):
    match = GIT_TAG_VERSION_RE.search(git_tag or '')
    if match is None:
        return None

    major, minor, patch, rc = match.groups()
    return (
        int(major),
        int(minor),
        int(patch or 0),
        0 if rc else 1,
        int(rc or 0),
    )


def git_tag_less_than(git_tag, version):
    git_tag_version = git_tag_version_key(git_tag)
    if git_tag_version is None:
        return False

    return git_tag_version < tuple(version) # + (1, 0)


def git_tag_greater_than(git_tag, version):
    git_tag_version = git_tag_version_key(git_tag)
    if git_tag_version is None:
        return False

    return git_tag_version >= tuple(version) # + (1, 0)


def add_config(config, package):
    if package not in config:
        config.append(package)


def has_config(config, package):
    return package in config

def config_matches_min_version(git_tag, version):
    git_tag_version = git_tag_version_key(git_tag)
    return git_tag_version is not None and not git_tag_less_than(git_tag, version)


def config_matches_max_version(git_tag, version):
    git_tag_version = git_tag_version_key(git_tag)
    return git_tag_version is not None and not git_tag_greater_than(git_tag, version)


def tag_tgz_path(path, git_tag):
    directory, filename = os.path.split(path)

    if filename.endswith('.tar.gz'):
        basename = filename[:-len('.tar.gz')]
        extension = '.tar.gz'
    else:
        basename, extension = os.path.splitext(filename)

    tags = []
    if git_tag:
        tags.append(re.sub(r'[^A-Za-z0-9._-]+', '_', git_tag))
    tags.append(platform.machine().lower())

    return os.path.join(directory, '{}-{}{}'.format(
        basename,
        '-'.join(tags),
        extension))


def qemu_target_list():
    if platform.machine().lower() in ("aarch64", "arm64"):
        return "aarch64-softmmu"

    return "x86_64-softmmu"

def pkg_config_variable(package, variable):
    try:
        value = subprocess.check_output(
            ["pkg-config", "--variable=" + variable, package]
        )
    except (OSError, subprocess.CalledProcessError):
        return None

    if isinstance(value, bytes):
        value = value.decode("utf-8")

    return value.strip()


def has_static_slirp():
    libdir = pkg_config_variable("slirp", "libdir")
    return bool(libdir and os.path.exists(os.path.join(libdir, "libslirp.a")))


def prepend_env_path(name, path):
    value = os.environ.get(name)
    if value:
        os.environ[name] = path + os.pathsep + value
    else:
        os.environ[name] = path


def build_static_slirp():
    script = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), LIBSLIRP_BUILD_SCRIPT
    )

    prefix = os.path.abspath("libslirp-static")
    src_dir = os.path.abspath("libslirp-src")
    build_dir = os.path.abspath("build-libslirp")

    cmd = [
        script,
        "--prefix",
        prefix,
        "--src-dir",
        src_dir,
        "--build-dir",
        build_dir,
    ]

    libslirp_ref = os.environ.get("LIBSLIRP_REF", LIBSLIRP_DEFAULT_REF)
    if libslirp_ref:
        cmd += ["--ref", libslirp_ref]

    run(cmd)

    prepend_env_path("PKG_CONFIG_PATH", os.path.join(prefix, "lib", "pkgconfig"))


def ensure_static_slirp(config):
    if not has_config(config, "--static") or not has_config(config, "--enable-slirp"):
        return

    if has_static_slirp():
        return

    build_static_slirp()

    if has_static_slirp():
        return

    raise RuntimeError(
        "Static QEMU with -netdev user support requires libslirp.a. "
        "build-libslirp-static.sh did not make it visible to pkg-config. "
        "Check the helper output or build QEMU without --static."
    )


def qemu_config(args, src_dir):
    config = list(QEMU_CONFIG)
    config.append('--target-list=' + qemu_target_list())

    for package, version in QEMU_CONFIG_MIN_VERSION:
        if config_matches_min_version(args.git_tag, version):
            add_config(config, package)

    for package, version in QEMU_CONFIG_MAX_VERSION:
        if config_matches_max_version(args.git_tag, version):
            add_config(config, package)

    return config


def run(args, **kwargs):
    print("+ '" + "' '".join(args) + "'")
    return subprocess.check_call(args, **kwargs)


def install_deps(args):
    run(['sudo', 'apt', 'install', '--no-install-recommends', '-y'] + QEMU_DEPS)


def preprocess(args):
    if args.src is None:
        args.src = os.path.abspath(
            os.path.join(os.getcwd(), 'qemu-' + args.git_tag if args.git_tag else 'src'))


def checkout(args):
    if not os.path.exists(args.src):
        os.mkdir(args.src)
    else:
        return
    #    raise RuntimeError("src path already exists {}".format(args.src))

    run(['git', 'clone', '--recursive', args.git, args.src])
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

        config = qemu_config(args, src_dir)
        ensure_static_slirp(config)

        help = subprocess.check_output(
            [src_dir + '/configure', '--help'], cwd=build_dir)

        if isinstance(help, bytes):
            help = help.decode("utf-8")

        logging.debug(help)

        # commit b10d49d7619e4957b4b971f816661b57e5061d71
        if 'libssh2' not in help:
            add_config(config, '--disable-libssh')
        else:
            add_config(config, '--disable-libssh2')

        run([src_dir + '/configure'] + config, cwd=build_dir)
        run(['make', '-j', str(os.sysconf('SC_NPROCESSORS_ONLN'))], cwd=build_dir)

        if os.path.isdir(args.out):
            run(['make', 'install', 'DESTDIR=' +
                 os.path.abspath(args.out) +
                 '/qemu'], cwd=build_dir)
        else:
            with tmpdir(prefix='out-' + os.path.basename(args.src)) as out_dir:
                target_dir = os.path.abspath(out_dir) + '/qemu'
                target = os.path.abspath(tag_tgz_path(args.out, args.git_tag))

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
        default="ssh://git@github.com:qemu/qemu.git")
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
