# Modifications Copyright (c) 2024 Ampere Computing LLC
# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Module containing nginx installation functions."""

import posixpath
from absl import flags

PACKAGE_NAME = "ampere_nginx"
APT_PACKAGES = (
    "pkg-config gcc g++ cmake autoconf automake libpcre3-dev "
    "libevent-dev pkg-config zlib1g-dev libssl-dev libfindbin-libs-perl "
    "libipc-run-perl git wget curl"
)

YUM_PACKAGES = (
    "cmake autoconf automake zlib-devel pcre-devel libevent-devel git wget curl"
)
DEPLOY_DIR = posixpath.join("/tmp", "nginx")
FLAGS = flags.FLAGS

flags.DEFINE_string(f"{PACKAGE_NAME}_version", "1.15.4", "nginx version")
flags.DEFINE_string(
    f"{PACKAGE_NAME}_data", None, "Location of HTML file and nginx conf"
)

flags.DEFINE_string(
    f"{PACKAGE_NAME}_local_html", "nginx/CF.html", "local test html"
)

flags.DEFINE_string(
    f"{PACKAGE_NAME}_conf",
    "nginx/nginx.conf.br",
    "The path to an Nginx config file that should be applied "
    "to the server instead of the default one.",
)
flags.DEFINE_string(
    f"{PACKAGE_NAME}_commit_tag",
    "a71f9312c2deb28875acc7bacfdd5695a111aa53",
    "git commit hash for nginx to build bginx with brotli compression"
    " git tag for gzip compression v1.0.0rc",
)
flags.DEFINE_string(
    f"{PACKAGE_NAME}_resty_version",
    "0.10.27",
    "git version for openresty lua-nginx module",
)
flags.DEFINE_string(
    f"{PACKAGE_NAME}_cflags", "-O3 -mcpu=native", "cflags to build nginx"
)


def GetnginxDirPath() -> str:
    return DEPLOY_DIR


def YumInstall(vm):
    """Installs nginx on the VM."""
    vm.Install("build_tools")
    vm.InstallPackages(YUM_PACKAGES)
    vm.InstallPackages("tcl-devel")
    vm.InstallPackages("perl")
    DownloadAndInstall(vm)


def DownloadAndInstall(vm):
    version = FLAGS[f"{PACKAGE_NAME}_version"].value
    out, _ = vm.RemoteCommand(
        f"curl http://nginx.org/download/nginx-{version}.tar.gz"
    )
    if "404 Not Found" in out:
        raise ValueError("Invalid nginx version.Cannot proceed")
    vm.RemoteCommand(f"mkdir -p {DEPLOY_DIR}")
    InstallNginx(vm, version)


def InstallNginx(vm, version):
    commit_hash = FLAGS[f"{PACKAGE_NAME}_commit_tag"].value
    resty_version = FLAGS[f"{PACKAGE_NAME}_resty_version"].value
    resty_tar = f'lua-nginx-module-{resty_version}.tar.gz'
    cflags = FLAGS[f"{PACKAGE_NAME}_cflags"].value
    vm.RemoteCommand(
        f"cd {DEPLOY_DIR} && wget http://nginx.org/download/nginx-{version}.tar.gz && "
        f"tar -xvf nginx-{version}.tar.gz"
    )
    vm.RemoteCommand(
        f"cd {DEPLOY_DIR} && git clone https://github.com/google/ngx_brotli.git && "
        f"cd ngx_brotli && git checkout {commit_hash} "
        " && git submodule update --init "
    )

    vm.RemoteCommand(
        f"cd {DEPLOY_DIR} && "
        f"wget https://github.com/openresty/lua-nginx-module/archive/v{resty_version}.tar.gz -O"
        f" {resty_tar} && "
        f"tar -xvf lua-nginx-module-{resty_version}.tar.gz"
    )

    vm.RemoteCommand(
        f"cd {DEPLOY_DIR} && git clone https://github.com/openssl/openssl.git && "
        f"git clone https://github.com/simpl/ngx_devel_kit.git"
    )
    vm.RemoteCommand(
        f"cd {DEPLOY_DIR} && wget https://github.com/LuaJIT/LuaJIT/archive/v2.1.0-beta3.tar.gz"
        f" -O LuaJIT-2.1.0-beta3.tar.gz && "
        f"tar -xvf LuaJIT-2.1.0-beta3.tar.gz && cd LuaJIT-2.1.0-beta3 && "
        f"make PREFIX={DEPLOY_DIR} && "
        f"sudo make install PREFIX={DEPLOY_DIR} && "
        f"sudo ln -sf luajit-2.1.0-beta3 {DEPLOY_DIR}/bin/luajit "
    )
    vm.RemoteCommand(
        f"cd {DEPLOY_DIR} && git clone https://github.com/openresty/lua-resty-core.git"
    )
    vm.RemoteCommand(
        f"cd {DEPLOY_DIR} && git clone https://github.com/openresty/stream-lua-nginx-module.git"
    )
    vm.RemoteCommand(
        f"cd {DEPLOY_DIR} && git clone https://github.com/openresty/lua-resty-lrucache.git"
    )
    vm.RemoteCommand(
        f"cd {DEPLOY_DIR} && git clone https://github.com/openresty/luajit2.git && "
        f"cd luajit2 && make PREFIX={DEPLOY_DIR} && sudo make install PREFIX={DEPLOY_DIR} && "
        f"sudo ln -sf luajit-2.1.0-beta3 {DEPLOY_DIR}/bin/luajit"
    )
    vm.RemoteCommand(
        f"cd {DEPLOY_DIR} && export LD_LIBRARY_PATH={DEPLOY_DIR}/lib/:$LD_LIBRARY_PATH && "
        f"export LUAJIT_LIB={DEPLOY_DIR}/lib && "
        f"export LUAJIT_INC={DEPLOY_DIR}/include/luajit-2.1 && "
        f"export ngx_addon_dir={DEPLOY_DIR}/ngx_brotli && cd {DEPLOY_DIR}/nginx-{version} && "
        f"./configure "
        f"--prefix={DEPLOY_DIR} "
        f'--with-cc-opt="{cflags}" '
        f'--with-ld-opt="-Wl,-rpath,$LUAJIT_LIB" '
        f"--with-http_ssl_module "
        f"--with-http_stub_status_module "
        f"--with-openssl={DEPLOY_DIR}/openssl "
        f"--with-http_v2_module "
        f"--add-module={DEPLOY_DIR}/ngx_devel_kit "
        f"--add-module={DEPLOY_DIR}/ngx_brotli "
        f"--add-module={DEPLOY_DIR}/lua-nginx-module-{resty_version} "
        f"&& cd {DEPLOY_DIR}/ngx_brotli/deps/brotli && mkdir out && cd out && "
        f"cmake .. && "
        f"make -j{vm.num_cpus} brotli && "
        f"cd {DEPLOY_DIR}/lua-resty-core && sudo make install PREFIX={DEPLOY_DIR} && "
        f"cd {DEPLOY_DIR}/lua-resty-lrucache && sudo make install PREFIX={DEPLOY_DIR} && "
        f"cd {DEPLOY_DIR}/nginx-{version} && make -j{vm.num_cpus} && make install"
    )


def AptInstall(vm):
    """Installs nginx on the VM."""
    vm.Install("build_tools")
    vm.InstallPackages(APT_PACKAGES)
    DownloadAndInstall(vm)


def Uninstall(vm):
    vm.RemoteCommand("sudo pkill -f nginx")
    vm.RemoteCommand(f"sudo rm -rf {DEPLOY_DIR}")
