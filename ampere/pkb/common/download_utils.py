# Copyright (c) 2024, Ampere Computing LLC
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


import os
import requests
import shutil
import hashlib
import perfkitbenchmarker
import posixpath
import logging
import subprocess
from typing import List
from tempfile import TemporaryDirectory

from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import data
from perfkitbenchmarker.virtual_machine import BaseVirtualMachine


INSTALL_DIR = '/opt/pkb'
CACHE_DIR = '/opt/pkb-cache'


def download_to_cache(url: str, md5: str = None, timeout: float = None) -> str:
    basename = os.path.basename(url)
    local_file = os.path.join(perfkitbenchmarker.__name__, 'data', basename)
    if os.path.isfile(local_file):
        if md5sum(local_file) != md5:
            os.remove(local_file)

    if not os.path.isfile(local_file):
        download_file(url, filename=local_file, md5=md5, timeout=timeout)
    return local_file


def download_file(url: str, *, filename: str = None, md5: str = None, timeout: float = None):
    filename = filename if filename else os.path.basename(filename)
    with requests.get(url, stream=True, timeout=timeout) as request:
        with open(filename, 'wb') as file:
            shutil.copyfileobj(request.raw, file)
    if not os.path.isfile(filename):
        raise OSError(f'{filename} was not found!')

    checksum = md5sum(filename)
    if md5 and md5 != checksum:
        raise Exception(f'{filename} checksum of {checksum} did not match expected {md5}')


def download(url: str, vms: List[BaseVirtualMachine], *,
             cache_dir: str = None,
             md5: str = None,
             sha256: str = None,
             dst: str = None,
             force: bool = False,
             timeout: float = None):
    """
    Downloads url file to the pkb host, then transfers to all VMs

    Args:
        url: https://your-file
        vms: List of target Machines
        cache_dir: Optionally select a directory on the PKB host to store the gsutil long-term and cache.
                   Defaults to temporary directory.
        md5: Optionally check the file against an md5 checksum
        sha256: Optionally check the file against an sha256 checksum
        dst: Optionally select target directory. Defaults to /opt/pkb-cache
        force: Always copy the file regardless if it's cached
        timeout: Subprocess timeout in seconds
    """
    with TemporaryDirectory() as scratch_dir:
        cache_dir = cache_dir if cache_dir else scratch_dir
        basename = posixpath.basename(url)
        src_file = posixpath.join(cache_dir, basename)
        dst = dst if dst else CACHE_DIR
        dst_file = posixpath.join(dst, basename)
        target_vms = [vm.RemoteCommand(f'ls {dst_file}', ignore_failure=True) for vm in vms]
        target_vms = [vm for (_, stderr), vm in zip(target_vms, vms) if stderr or force]
        if not target_vms:
            return

        if force or not data.ResourceExists(src_file):
            logging.debug(f'Fetching {url}')
            download_file(url, filename=src_file, timeout=timeout)
        if not data.ResourceExists(src_file):
            raise errors.Setup.InvalidSetupError(f'Failed to fetch file {url}')

        if md5:
            check = md5sum(src_file)
            if check != md5:
                raise errors.Setup.InvalidSetupError(f'MD5 sum {check} didn\'t match expected: {md5}')
        if sha256:
            check = sha256sum(src_file)
            if check != sha256:
                raise errors.Setup.InvalidSetupError(f'SHA256 sum {check} didn\'t match expected: {sha256}')

        for vm in target_vms:
            mk_cache_dir(vm)
            vm.PushFile(src_file, dst_file)
    return dst_file


def gsutil(uri: str, vms: List[BaseVirtualMachine], *,
           cache_dir: str = None,
           md5: str = None,
           sha256: str = None,
           dst: str = None,
           force: bool = False,
           timeout: float = None):
    """
    Downloads gsutil file to the pkb host, then transfers to all VMs

    Args:
        uri: gs://your-file
        vms: List of target Machines
        cache_dir: Optionally select a directory on the PKB host to store the gsutil long-term and cache.
                   Defaults to temporary directory.
        md5: Optionally check the file against an md5 checksum
        sha256: Optionally check the file against an sha256 checksum
        dst: Optionally select target directory. Defaults to /opt/pkb-cache
        force: Always copy the file regardless if it's cached
        timeout: Subprocess timeout in seconds
    """
    with TemporaryDirectory() as scratch_dir:
        cache_dir = cache_dir if cache_dir else scratch_dir
        basename = posixpath.basename(uri)
        src_file = posixpath.join(cache_dir, basename)
        dst = dst if dst else CACHE_DIR
        dst_file = posixpath.join(dst, basename)
        target_vms = [vm.RemoteCommand(f'ls {dst_file}', ignore_failure=True) for vm in vms]
        target_vms = [vm for (_, stderr), vm in zip(target_vms, vms) if stderr or force]
        if not target_vms:
            return

        if force or not data.ResourceExists(src_file):
            logging.debug(f'Fetching {uri}')
            subprocess.check_output(['gsutil', 'cp', uri, src_file],
                                    stderr=subprocess.STDOUT,
                                    timeout=timeout)
        if not data.ResourceExists(src_file):
            raise errors.Setup.InvalidSetupError(f'Failed to fetch file {uri}')

        if md5:
            check = md5sum(src_file)
            if check != md5:
                raise errors.Setup.InvalidSetupError(f'MD5 sum {check} didn\'t match expected: {md5}')
        if sha256:
            check = sha256sum(src_file)
            if check != sha256:
                raise errors.Setup.InvalidSetupError(f'SHA256 sum {check} didn\'t match expected: {sha256}')

        for vm in target_vms:
            mk_cache_dir(vm)
            vm.PushFile(src_file, dst_file)
    return dst_file


def mk_cache_dir(vm: BaseVirtualMachine, cache_dir: str = CACHE_DIR):
    vm.RemoteCommand(f'sudo mkdir -p {cache_dir}')
    vm.RemoteCommand(f'sudo chown -R $USER:$USER {cache_dir}')


def md5sum(filename, *, block_size=2**20) -> str:
    m = hashlib.md5()
    with open(filename, "rb") as f:
        while True:
            block = f.read(block_size)
            if not block:
                break
            m.update(block)
    return m.hexdigest()


def sha256sum(filename: str, *, block_size=2**20) -> str:
    m = hashlib.sha256()
    with open(filename, "rb") as f:
        while True:
            block = f.read(block_size)
            if not block:
                break
            m.update(block)
    return m.hexdigest()


def send_resource_to_vm(vm, path):
    """Get resource from private GCS bucket or local path, store in local temp dir, send to VM
        NOTE:   **IMPORTANT** system running PKB must have an authorized gcloud account 
                with access to the GCS bucket if using a gs:// path
    Params:
        vm:     The BaseVirtualMachine object representing the VM.
        path:   Either in the form gs://<bucket-name>/<file-name>, absolute path, or root path ./
    Returns:
        deploy_file: path to the file on the VM if successful, otherwise raises error
    """
    DOWNLOAD_LOC = vm_util.GetTempDir()
    if 'gs://' in path:
        cmd = f'gsutil cp {path} {DOWNLOAD_LOC}'
    else:
        cmd = f'cp {data.ResourcePath(path)} {DOWNLOAD_LOC}'

    # Copy resource to temp dir from GCS or absolute local path (if doesn't exist)
    file_name = posixpath.basename(path)
    local_path = posixpath.join(DOWNLOAD_LOC, file_name)
    if not data.ResourceExists(local_path): 
        try:
            stdout = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
            logging.debug(f'Successfully copied resources from {path} to {DOWNLOAD_LOC}: {stdout.decode()}')
        except subprocess.CalledProcessError as e:
            if e.returncode == 127: # gsutil not found
                raise errors.Setup.InvalidSetupError(f'{e.stdout.decode()}')
            else:
                raise errors.Setup.InvalidSetupError(f'Copying resources from {path} to {DOWNLOAD_LOC} '
                                                     f'failed with return code {e.returncode}: {e.stdout.decode()}')
    else:
        logging.debug(f'Resource {local_path} already exists on the local system.')

    # Send resource to VM (if doesn't exist)
    deploy_file = posixpath.join(CACHE_DIR, file_name)
    _, _, retcode = vm.RemoteCommandWithReturnCode(f'test -f {deploy_file}', ignore_failure=True)
    file_exists = retcode == 0
    if not file_exists: 
        logging.debug(f'Copying resource {deploy_file} to VM...')
        LOCAL_FILE = posixpath.join(DOWNLOAD_LOC, file_name)
        if not os.path.exists(LOCAL_FILE):
            raise errors.Setup.InvalidSetupError(f'Resource from {path} does not exist: {LOCAL_FILE}')
        vm.RemoteCommand(f'sudo mkdir -p {CACHE_DIR}')
        vm.RemoteCommand(f'sudo chown -R $USER:$USER {CACHE_DIR}')
        vm.PushFile(LOCAL_FILE, CACHE_DIR)
    else:
        logging.debug(f'Resource {deploy_file} already exists on the VM.')
    
    _, _, retcode = vm.RemoteCommandWithReturnCode(f'test -f {deploy_file}', ignore_failure=True) # Verify file copied successfully
    file_exists = retcode == 0
    if file_exists:
        return deploy_file
    raise errors.Setup.InvalidSetupError(f'Failed to copy resource from {path} to VM at path: {deploy_file}')
