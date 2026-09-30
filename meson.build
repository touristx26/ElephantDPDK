# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2026 ElephantDPDK contributors

project('elephantdpdk', 'c',
    version: '0.1.0',
    license: 'BSD-3-Clause',
    default_options: [
        'buildtype=release',
        'c_std=gnu11',
        'warning_level=2',
    ])

cc = meson.get_compiler('c')

# getopt_long() and the CPU affinity APIs used via DPDK require GNU
# extensions on glibc; gcc >= 14 treats implicit declarations as errors.
if host_machine.system() == 'linux'
    add_project_arguments('-D_GNU_SOURCE', language: 'c')
endif

# Link against an existing DPDK build via its libdpdk.pc (pkg-config).
# Point PKG_CONFIG_PATH at the DPDK build directory, e.g.:
#   export PKG_CONFIG_PATH=$HOME/dpdk/build/meson-uninstalled:$PKG_CONFIG_PATH
# or use an installed DPDK, where libdpdk.pc is already on the default path.
dpdk_dep = dependency('libdpdk', method: 'pkg-config', required: false)
if not dpdk_dep.found()
    dpdk_dep = dependency('dpdk', method: 'pkg-config')
endif

# libm for pow() in the NitroSketch seeding code.
m_dep = cc.find_library('m', required: false)

sources = files(
    'src/main.c',
    'src/app_ctx.c',
    'src/rx/rx.c',
    'src/nitrosketch/nitrosketch.c',
    'src/dispatcher/dispatcher.c',
    'src/worker/worker.c',
)

inc = include_directories('src')

extra_flags = []
if cc.has_argument('-Wno-format-truncation')
    extra_flags += '-Wno-format-truncation'
endif

executable('elephantdpdk',
    sources,
    include_directories: inc,
    dependencies: [dpdk_dep, m_dep],
    c_args: extra_flags,
    install: false)
