# Copyright (c) 2024, Ampere Computing LLC
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


"""
Module containing openssl installation and cleanup functions.

"""

import posixpath
from absl import flags
from ampere.pkb.common import download_utils


FLAGS = flags.FLAGS

PACKAGE_NAME = "ampere_openssl"

openssl_dir = posixpath.join(download_utils.INSTALL_DIR, "openssl")

flags.DEFINE_bool(f"{PACKAGE_NAME}_use", False, "install openSSL")

openssl_version = flags.DEFINE_string(
    f"{PACKAGE_NAME}_version", "1.1.1v", "Openssl Version."
)

openssl_cflag = flags.DEFINE_string(
    f"{PACKAGE_NAME}_cflag", "-O2 -mcpu=native", "define cflag"
)


def Install(vm):
    """
    Installs the Openssl package on the VM.
    """
    openssl_ver = openssl_version.value
    openssl_cflag_val = openssl_cflag.value
    vm.Install("wget")
    vm.Install("build_tools")
    openssl_url = ""
    openssl_install_dir = posixpath.join(
        download_utils.INSTALL_DIR, f"openssl-{openssl_ver}"
    )
    if openssl_ver.startswith("1"):
        url_ver= openssl_ver.replace(".","_")
        openssl_url = f"https://github.com/openssl/openssl/releases/download/OpenSSL_{url_ver}/openssl-{openssl_ver}.tar.gz"
    else:
        openssl_url = f"https://github.com/openssl/openssl/releases/download/openssl-{openssl_ver}/openssl-{openssl_ver}.tar.gz"
    opensssl_tar_dir = f"openssl-{openssl_ver}.tar.gz"
    vm.RemoteCommand(f"cd {download_utils.INSTALL_DIR}; wget {openssl_url}")
    vm.RemoteCommand(f"cd {download_utils.INSTALL_DIR}; tar -xzf {opensssl_tar_dir}")
    patch_link = ""
    if openssl_ver.startswith("3.3"):
        patch_link = f"0001-Enable-SHA3-unrolling-and-EOR3-optimization-for-Ampere-3.3.0.patch"
    elif openssl_ver.startswith("3.2"):
        patch_link = f"0001-Enable-SHA3-unrolling-and-EOR3-optimization-for-Ampere-3.2.0.patch"       
    if patch_link != "":
        vm.RemoteCopy(f"./ampere/pkb/data/openssl/{patch_link}", f"{openssl_install_dir}/{patch_link}")
        vm.RemoteCommand(f"cd {openssl_install_dir}; patch -p1 <{patch_link}")
    vm.RemoteCommand(
        f'cd {openssl_install_dir}; CFLAGS="{openssl_cflag_val}" ./config --prefix={openssl_dir}'
    )
    vm.RemoteCommand(f"cd {openssl_install_dir};  make  -j `nproc`")
    vm.RemoteCommand(f"cd {openssl_install_dir}; sudo make  -j `nproc` install")


def Uninstall(vm):
    """
    Remove Openssl package on the VM.
    """
    openssl_ver = openssl_version.value
    openssl_install_dir = posixpath.join(
        download_utils.INSTALL_DIR, f"openssl-{openssl_ver}"
    )
    vm.RemoteCommand(f"sudo rm -rf {openssl_install_dir}", ignore_failure=True)
    vm.RemoteCommand(f"sudo rm -rf {openssl_dir}", ignore_failure=True)
