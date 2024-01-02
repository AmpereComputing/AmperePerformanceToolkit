# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for NFS service."""

import unittest
from unittest import mock

from absl import flags
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import nfs_service
from tests import pkb_common_test_case


FLAGS = flags.FLAGS
_DEFAULT_NFS_TIER = 'foo'


class _DemoNfsService(nfs_service.BaseNfsService):
  CLOUD = 'mock'
  NFS_TIERS = (_DEFAULT_NFS_TIER,)

  def __init__(self, disk_spec, zone):
    super(_DemoNfsService, self).__init__(disk_spec, zone)
    self.is_ready_called = False

  def _IsReady(self):
    return True

  def GetRemoteAddress(self):
    return 'remote1'

  def _Create(self):
    pass

  def _Delete(self):
    pass


class _DemoNfsServiceWithDefaultNfsVersion(_DemoNfsService):
  CLOUD = 'mock2'
  DEFAULT_NFS_VERSION = '4.1'


class NfsServiceTest(pkb_common_test_case.PkbCommonTestCase):

  def _SetFlags(self, nfs_tier=''):
    FLAGS['default_timeout'].parse(10)
    FLAGS['nfs_tier'].parse(nfs_tier)

  def _NewNfsResource(self, nfs_tier=''):
    self._SetFlags(nfs_tier=nfs_tier)
    return _DemoNfsService(disk.BaseNFSDiskSpec('test_component'), 'us-west1-a')

  def testNewNfsResource(self):
    nfs = self._NewNfsResource(_DEFAULT_NFS_TIER)
    self.assertEqual(_DEFAULT_NFS_TIER, nfs.nfs_tier)
    self.assertIsNone(nfs.DEFAULT_NFS_VERSION)

  def testNewNfsResourceBadNfsTier(self):
    with self.assertRaises(errors.Config.InvalidValue):
      self._NewNfsResource('NonExistentNfsTier')

  def testNewNfsResourceNfsTierNotSet(self):
    nfs = self._NewNfsResource()
    self.assertIsNone(nfs.nfs_tier)

  def testRegistry(self):
    nfs_class = nfs_service.GetNfsServiceClass(_DemoNfsService.CLOUD)
    self.assertEqual(_DemoNfsService, nfs_class)

  def testCreateNfsDisk(self):
    nfs = self._NewNfsResource()
    nfs_disk = nfs.CreateNfsDisk()
    self.assertEqual('remote1:/', nfs_disk.device_path)
    self.assertIsNone(nfs_disk.nfs_version)

  def testDefaultNfsVersion(self):
    self._SetFlags()
    nfs = _DemoNfsServiceWithDefaultNfsVersion(
        disk.BaseNFSDiskSpec('test_component'), 'us-west1-a'
    )
    nfs_disk = nfs.CreateNfsDisk()
    self.assertEqual('4.1', nfs_disk.nfs_version)


class UnmanagedNfsServiceTest(pkb_common_test_case.PkbCommonTestCase):

  def _setUpDiskSpec(self):
    disk_spec = disk.BaseNFSDiskSpec('test_disk_spec')
    disk_spec.device_path = '/test_dir'
    self.disk_spec = disk_spec

  def _setUpMockServerVm(self):
    self.mock_server_vm = mock.Mock(internal_ip='1.1.1.1')
    self.mock_server_vm.RemoteCommand.return_value = None, None, None

  def setUp(self):
    super(UnmanagedNfsServiceTest, self).setUp()
    self._setUpDiskSpec()
    self._setUpMockServerVm()
    self.nfs_service = nfs_service.UnmanagedNfsService(
        self.disk_spec, self.mock_server_vm
    )

  def testNewUnmanagedNfsService(self):
    self.assertIsNotNone(self.nfs_service)
    self.assertIsNotNone(self.nfs_service.server_vm)
    self.assertIsNotNone(self.nfs_service.disk_spec)
    self.assertEqual(
        self.nfs_service.server_directory, self.disk_spec.device_path
    )

  def testCreateNfsDisk(self):
    nfs_disk = self.nfs_service.CreateNfsDisk()
    self.assertEqual(nfs_disk.device_path, '1.1.1.1:/test_dir')

  def testGetRemoteAddress(self):
    self.assertEqual(self.nfs_service.GetRemoteAddress(), '1.1.1.1')

  def testNfsExportDirectoryFirstTime(self):
    vm = mock.Mock(BASE_OS_TYPE='debian')
    vm.TryRemoteCommand.return_value = False
    nfs_service.NfsExport(vm, '/foo/bar')
    # /etc/exports updated
    self.assertLen(vm.RemoteCommand.call_args_list, 3)
    exportfs_cmd = vm.RemoteCommand.call_args_list[0][0][0]
    self.assertRegex(exportfs_cmd, 'tee -a /etc/exports')
    vm.RemoteCommand.assert_has_calls([
        mock.call('sudo systemctl restart nfs-kernel-server'),
        mock.call('sudo systemctl enable nfs', ignore_failure=True),
    ])

  def testNfsExportDirectoryAlreadyExported(self):
    # Testing when NfsExport called twice with the same path.
    vm = mock.Mock(BASE_OS_TYPE='rhel')
    vm.TryRemoteCommand.return_value = True
    nfs_service.NfsExport(vm, '/foo/bar')
    # RemoteCommand not called with the mkdir ... echo calls
    self.assertLen(vm.RemoteCommand.call_args_list, 2)
    vm.RemoteCommand.assert_has_calls([
        mock.call('sudo systemctl restart nfs-server'),
        mock.call('sudo systemctl enable nfs', ignore_failure=True),
    ])

  def testNfsExportAndMount(self):
    mock_nfs_create = self.enter_context(
        mock.patch.object(nfs_service, 'UnmanagedNfsService')
    )
    headnode = mock.Mock(internal_ip='10.0.1.11')
    vm1 = mock.Mock(user_name='perfkit')
    vm2 = mock.Mock(user_name='perfkit')

    nfs_service.NfsExportAndMount(
        [headnode, vm1, vm2], '/client_path', '/server_path'
    )

    mock_nfs_create.assert_called_with(None, headnode, False, '/server_path')
    mount_cmd = (
        'sudo mkdir -p /client_path; '
        'sudo chown perfkit /client_path; '
        'echo "10.0.1.11:/server_path /client_path nfs defaults 0 0\n" '
        '| sudo tee -a /etc/fstab; sudo mount -a'
    )
    for vm in (vm1, vm2):
      vm.Install.assert_called_with('nfs_utils')
      vm.RemoteCommand.assert_called_with(mount_cmd)


if __name__ == '__main__':
  unittest.main()
