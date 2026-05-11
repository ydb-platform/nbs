<!--
SPDX-FileCopyrightText: 2022 EfficiOS Inc.

SPDX-License-Identifier: MIT
-->

Library for Restartable Sequences
=================================

by Mathieu Desnoyers


Required and optional dependencies
----------------------------------

The following dependencies are optional:

  - [libnuma](https://github.com/numactl/numactl)
    To build without this dependency run `./configure` with `--disable-numa`
  - [libseccomp](https://github.com/seccomp/libseccomp/)

Building
--------

### Prerequisites

This source tree is based on the Autotools suite from GNU to simplify
portability. Here are some things you should have on your system in order to
compile the Git repository tree:

  - [GNU Autotools](http://www.gnu.org/software/autoconf/)
    (**Automake >= 1.12**, **Autoconf >= 2.69**,
    **Autoheader >= 2.69**;
    make sure your system-wide `automake` points to a recent version!)
  - **[GNU Libtool](https://www.gnu.org/software/libtool/) >= 2.2**
  - **[GNU Make](https://www.gnu.org/software/make/)**
  - **[pkg-config](https://www.freedesktop.org/wiki/Software/pkg-config)**
  - **Linux kernel headers** from kernel **>= 4.18** to build on x86, arm,
    ppc, and mips and from kernel **>= 4.19** to build on s390.


### Building steps

If you get the tree from the Git repository, you will need to run

    ./bootstrap

in its root. It calls all the GNU tools needed to prepare the tree
configuration.

To build and install, do:

    ./configure
    make
    sudo make install
    sudo ldconfig

**Note:** the `configure` script sets `/usr/local` as the default prefix for
files it installs. However, this path is not part of most distributions'
default library path, which will cause builds depending on `librseq`
to fail unless `-L/usr/local/lib` is added to `LDFLAGS`. You may provide a
custom prefix to `configure` by using the `--prefix` switch
(e.g., `--prefix=/usr`).


### Building against a local version of the kernel headers

    cd /path/to/kernel/sources
    make headers_install
    cd /path/to/librseq
    CPPFLAGS=-I/path/to/kernel/sources/usr/include ./configure
    make
    sudo make install
    sudo ldconfig

### Running the test suite

To run the test suite after a successful build:

    make check

The default list of tests to run can be overriden by the TESTS variable:

    make check TESTS="run_unregistered_test.tap"

