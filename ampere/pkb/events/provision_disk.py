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

import logging
from typing import Any
from absl import flags
from perfkitbenchmarker import events
from perfkitbenchmarker import stages
from perfkitbenchmarker import errors
from perfkitbenchmarker.virtual_machine import VirtualMachine
from perfkitbenchmarker.benchmark_spec import BenchmarkSpec

FLAGS = flags.FLAGS

flags.DEFINE_bool(
    "ampere_baremetal_disk", False, help="Enable baremetal disk provisioning"
)
flags.DEFINE_integer(
    "ampere_baremetal_num_disks", 1, help="Number of disks to provision"
)
flags.DEFINE_integer("ampere_baremetal_minimum_disksize", 100, help="disk size in GB")
flags.DEFINE_integer(
    "ampere_num_partitions_per_disk", 1, help="Number of partitions per disk"
)
flags.DEFINE_list(f"ampere_disks", [], "disk names like /dev/nvme8n1 ")
flags.DEFINE_bool("ampere_format_disks", True, help="Format the disks")
flags.DEFINE_string("ampere_format_type", "ext4", help="format type ext4 or xfs")
flags.DEFINE_bool("ampere_is_ramdisk", False, help="Should a RAM disk be provisioned")
flags.DEFINE_integer(
            "ampere_num_ramdisks", 1, help="Number of ramdisks to provision"
            )
flags.DEFINE_string("ampere_ramdisk_size", "48g", help="Ram Disk Size")
flags.DEFINE_string("ampere_ramdisk_mount_point", "/mnt/ramdisk", help="Ramdisk mountpoint")
flags.DEFINE_bool(
    "ampere_remove_disk", True, help="remove mountpoint and umount disks attached using this facility"
)


def register_all(_: Any, parsed_flags: flags.FLAGS):
    events.after_phase.connect(after_phase, weak=False)
    events.before_phase.connect(before_phase, weak=False)


def before_phase(sender: Any, benchmark_spec: BenchmarkSpec):
    if not FLAGS.ampere_baremetal_disk:
        return
    if sender == stages.TEARDOWN and FLAGS.ampere_remove_disk:
        server_vm = benchmark_spec.vm_groups["servers"][0]
        return perform_disk_umount(server_vm)

def after_phase(sender: Any, benchmark_spec: BenchmarkSpec):
    if not FLAGS.ampere_baremetal_disk:
        return
    if sender == stages.PROVISION:
        server_vm = benchmark_spec.vm_groups["servers"][0]
        return perform_disk_mount(server_vm)


def _format_disk(local_vm: VirtualMachine, list_disks: list):
    # delete all partitions
    logging.info("delete all partitions")
    for disk_format in list_disks:
        delete_cmd = f'echo "o\nw" | sudo fdisk {disk_format} > /dev/null 2>&1'
        local_vm.RemoteCommand(delete_cmd)

def _sizeofdisks(local_vm: VirtualMachine,list_disks: list) -> list:
    size_disks = []
    for disk_selected in list_disks:
        remote_cmd = f"lsblk -o name,size -as {disk_selected} -n -b"
        stdout, _ = local_vm.RemoteCommand(remote_cmd)
        split_line = stdout.split()
        size_disk = {}
        size_disk['name'] = disk_selected
        size_disk['size'] = int(int(split_line[1]) // (1024*1024*1024)) - 5
        size_disks.append(size_disk)
    return size_disks

def _partition_disk(local_vm: VirtualMachine, list_disks: list, size_disks: list) -> list:
    # partition the disks
    num_disks = 0
    partition_disks = []
    add_extended_disk = False
    format_make_partition = False
    logging.info(size_disks)
    for disk_partition in size_disks:
        disk_name = disk_partition['name']
        disk_size = disk_partition['size']
        logging.info(disk_partition)
        partition_type = 'p'
        num_partitions = FLAGS.ampere_num_partitions_per_disk
        disk_partition_size = int(int(disk_size) // FLAGS.ampere_num_partitions_per_disk)
#if partitions > 4 then 4th partition extended and not included in mounted disks
        if FLAGS.ampere_num_partitions_per_disk > 4:
            num_partitions = FLAGS.ampere_num_partitions_per_disk + 1
            add_extended_disk = True
        for part in range(0, FLAGS.ampere_num_partitions_per_disk):
            last_sector = f"+{disk_partition_size}G"
            partition_command = (
                f'echo "n\n{partition_type}\n{part+1}\n\n{last_sector}\nY\nw\n" | sudo fdisk {disk_name}'
            )
            if add_extended_disk and part == 3:
                partition_type = 'e'
                partition_command = (
                        f'echo "n\n{partition_type}\n{part+1}\n\n\n\nY\nw\n" | sudo fdisk {disk_name}'
                        )

            partition, _ = local_vm.RemoteCommand(partition_command)
            if "Syncing disks." in partition.strip():
                if add_extended_disk and part == 3:
                    format_make_partition = False
                else:
                    format_make_partition = True
            if format_make_partition:
                part_command = f'sudo fdisk -l {disk_name} | tail -n 1 |cut -d " " -f1'
                format_partition, _ = local_vm.RemoteCommand(part_command)
                format_partition = format_partition.strip()
                # filesystem
                fs_cmd = f"sudo mkfs.{FLAGS.ampere_format_type} {format_partition}"
                local_vm.RemoteCommand(fs_cmd)
                partition_disk = {}
                partition_disk['name'] = format_partition
                partition_disk['size'] = disk_partition_size
                partition_disks.append(partition_disk)
    return partition_disks

def _make_dirs(local_vm: VirtualMachine, mount_dir: str):
    prepare_mount_cmd = (f"sudo rm -f {mount_dir};sudo mkdir -p {mount_dir}")
    local_vm.RemoteCommand(prepare_mount_cmd)

def _set_permissions_mountpoints(local_vm: VirtualMachine, mount_dir: str):
    user_name = local_vm.user_name
    logging.info(f"server user_name {user_name}")
    local_vm.RemoteCommand(f"sudo chmod 777 {mount_dir};sleep 2;"
            f"sudo chown -R {user_name}:root {mount_dir};sleep 2;")

#Handled mounting disk and ramdisk separately
#if disk and ramdisk are to be attached 
#TODO both conditions are true 
#in mount_ramdisk don't append scratchdisk to vm.scratch_disks
#return scratchdisk from mount_ramdisk function
#In mount_file_system function check if both flags are true then
#add ramdisk after bm disk in local_vm.scratch_disks

def _mount_file_system(local_vm: VirtualMachine, list_disks: list, partitions: list):
    mount_disk = 0
    disks = []
    for disk_attached in list_disks:
        for part in range(0, FLAGS.ampere_num_partitions_per_disk):
            mounted_disk = "/mnt/disk" + str(mount_disk)
            _make_dirs(local_vm, mounted_disk)
            partition_name = partitions[mount_disk]['name']
            mount_cmd = f"sudo mount {partition_name} {mounted_disk}"
            local_vm.RemoteCommand(mount_cmd)
            _set_permissions_mountpoints(local_vm, mounted_disk)
            disks.append(f"{mounted_disk}")
            metadata1 = {
                "num_disk": part + 1,
                "disk_name": partition_name,
                "mount_point": f"{mounted_disk}",
            }
            scratchdisk = DiskStatic(f"{mounted_disk}", metadata1)
            mount_disk += 1
            local_vm.scratch_disks.append(scratchdisk)

def _check_raid0_disk(local_vm:VirtualMachine, disk_name:str, disk_type:str) -> str:
    raid0_disk = ""
    if disk_type in ["raid0", "md"]:
        parent_cmd = (f'lsblk -as /dev/{disk_name}| '
                f'grep disk | cut -d "─" -f2 | cut -d" " -f1')
        raid0_disk, _ = local_vm.RemoteCommand(parent_cmd)
        raid0_disk = raid0_disk.strip()
       # logging.info("raid0_disk present", raid0_disk)
    return raid0_disk


def detect_disks(local_vm: VirtualMachine) -> list:
    remote_cmd = "lsblk -o NAME,MOUNTPOINTS,SIZE,TYPE -n -l"
    stdout, _ = local_vm.RemoteCommand(remote_cmd)
    lines = stdout.splitlines()
    list_of_disks = []
    raid0_disk = ""
    boot_disk = ""
    for line in lines:
        proceed = False
        # Split the line into device name, type, file system type, and partition type
        disk_details = line.split()
        if len(disk_details) < 1:
            continue
        logging.info("enters if loop")
        raid0_disk = _check_raid0_disk(local_vm, disk_details[0], disk_details[2])
        if (disk_details[2] in ["raid0", "md","rom"] or 
                (disk_details[1] == "0B" or "zram" in disk_details[0])):
            continue
        disk_name = disk_details[0]
        logging.info(f"disk_name====={disk_name}")
        proceed, parent_disk = _get_parent_disk(local_vm, disk_name, raid0_disk)
        if not proceed:
            continue
        logging.info(f"parent_disk====={parent_disk}")
        logging.info(f"proceed====={proceed}")
        list_of_disks, boot_disk = remove_boot_disk(list_of_disks, disk_details, parent_disk, boot_disk)
        list_of_disks = _update_list_of_disks(list_of_disks, parent_disk)

    logging.info(f'returning detected {len(list_of_disks)} {list_of_disks}')
    return list_of_disks

def remove_boot_disk(_list_of_disks: list, _disk_details: list, _parent_disk: str, _boot_disk: str) -> tuple:
    if ((len(_disk_details) >= 3 and (_disk_details[1] in ["/","/boot","/home","[SWAP]"])) and 
            (_boot_disk != _parent_disk and (f"/dev/{_parent_disk}" in _list_of_disks))):
        logging.info("enters first if loop")
        _list_of_disks.remove(f'/dev/{_parent_disk}')
        _boot_disk = _parent_disk
    elif (_disk_details[2] == "part" and _boot_disk == _parent_disk and 
            (f"/dev/{_parent_disk}" in _list_of_disks)):
        logging.info("enters elif loop")
        _list_of_disks.remove(f'/dev/{_parent_disk}')
        _boot_disk = _parent_disk
    return _list_of_disks, _boot_disk


def _update_list_of_disks(_list_of_disks: list, _parent_disk: str) -> list:
    if (f"/dev/{_parent_disk}" not in _list_of_disks and len(FLAGS.ampere_disks) > 0):
        logging.info("enters ampere_disks if loop")
        _list_of_disks.append(f'/dev/{_parent_disk}')
        logging.info(_list_of_disks)
    elif (f"/dev/{_parent_disk}" not in _list_of_disks and len(FLAGS.ampere_disks) == 0 and 
            len(_list_of_disks) < FLAGS.ampere_baremetal_num_disks):
        logging.info(f"enters baremetal_num_disks if loop /dev/{_parent_disk}")
        _list_of_disks.append(f'/dev/{_parent_disk}')
        logging.info(f'updating {len(_list_of_disks)} {_list_of_disks}')
    return _list_of_disks


def _get_parent_disk(local_vm: VirtualMachine,disk_name: str,raid0_disk: list) -> tuple:
    proceed = False
    #nvme
    parent_cmd = (
        f'lsblk -as /dev/{disk_name}| grep disk |'        
        ' cut -d " " -f1 | sed -e "s/└─//g"'
    )
    parent_disk, _, iRet = local_vm.RemoteCommandWithReturnCode(parent_cmd)
    if iRet == 1:
        #ssd
        parent_cmd = (
                f'lsblk -as /dev/{disk_name}| grep disk |'
                ' cut -d " " -f1')
        parent_disk, _ = local_vm.RemoteCommand(parent_cmd)
    parent_disk = parent_disk.strip()
    if (len(parent_disk) > 0 and 
            len(raid0_disk) > 0 and
            parent_disk not in raid0_disk):
        proceed = True
    elif (len(parent_disk) > 0 and len(raid0_disk) == 0):
        proceed = True
    #logging.info("proceed, parent_disk",proceed, parent_disk)
    return proceed, parent_disk

def _mount_ramdisks(local_vm: VirtualMachine):
    for ramdisk in range(0,FLAGS.ampere_num_ramdisks):
        mounted_disk = FLAGS.ampere_ramdisk_mount_point + str(ramdisk)
        _make_dirs(local_vm, mounted_disk)
        remote_cmd = (
                        f"sudo mount -t tmpfs -o size={FLAGS.ampere_ramdisk_size}"
                        f",mpol=prefer:0 tmpfs {mounted_disk}")
        local_vm.RemoteCommand(remote_cmd)
        metadata1 = {"num_disk": ramdisk + 1,
                "ramdisk_mount": f"{mounted_disk}",
                }
        scratchdisk = DiskStatic(f"{mounted_disk}",
                metadata1)
        local_vm.scratch_disks.append(scratchdisk)

def _check_ramdisksize(server_vm: VirtualMachine) -> bool:
    valid = True
    mem_size, err, iRet = server_vm.RemoteCommandWithReturnCode(
    "sudo free -t -g | grep 'Mem' |  sed -r 's/[[:blank:]]+/ /g' | cut -d' ' -f2",
    ignore_failure=True,)
    mem_size = mem_size.strip()
    if FLAGS.ampere_ramdisk_size != "":
        if "T" in FLAGS.ampere_ramdisk_size:
            temp1_ramdisk_size = (int(FLAGS.ampere_ramdisk_size[:-1]) * 1024)
        elif "M" in FLAGS.ampere_ramdisk_size:
            temp1_ramdisk_size = int(int(FLAGS.ampere_ramdisk_size[:-1]) // 1024)
        elif "G" in FLAGS.ampere_ramdisk_size:
            temp1_ramdisk_size = int(FLAGS.ampere_ramdisk_size[:-1])
        if (temp1_ramdisk_size * FLAGS.ampere_num_ramdisks) >= int(mem_size) * 0.8:
            logging.info(f'{FLAGS.ampere_num_ramdisks} Ramdisks cannot be created')
            valid = False
    return valid

def _disk_mount_validations(list_disks: list) -> str:
    sErrorMessage = "valid"
    if len(list_disks) == 0:
        logging.info(f'Disks are not attached to server')
        sErrorMessage = "Disks are not attached to server."
    if len(FLAGS.ampere_disks) == 0 and FLAGS.ampere_baremetal_num_disks == 0:
        raise ValueError(f'ampere_baremetal_num_disks should be mentioned')
    if len(FLAGS.ampere_disks) > 0 and FLAGS.ampere_baremetal_num_disks != len(FLAGS.ampere_disks):
        logging.info(f'{FLAGS.ampere_baremetal_num_disks} disks should be mentioned'
                ' in list of ampere_disks')
        sErrorMessage = f'{FLAGS.ampere_baremetal_num_disks} disks should be mentioned'
        'in list of ampere_disks'
    elif len(FLAGS.ampere_disks) > 0 and set(FLAGS.ampere_disks).issubset(set(list_disks)):
        sErrorMessage = 'valid'
    elif len(FLAGS.ampere_disks) == 0 and FLAGS.ampere_baremetal_num_disks > 0:
        sErrorMessage = 'valid'
    else:
        logging.info(f'Failed to find the disks mentioned in list of ampere_disks'
                f'{FLAGS.ampere_disks}')
        sErrorMessage = f'Failed to find the disks {list_disks} mentioned in '
        f'list of ampere_disks {FLAGS.ampere_disks}'
    return sErrorMessage


def perform_disk_mount(server_vm: VirtualMachine):
    if FLAGS.ampere_is_ramdisk:
        proceed = _check_ramdisksize(server_vm)
        if not proceed:
            raise ValueError(f'{FLAGS.ampere_num_ramdisks} Ramdisks cannot be created')
        # Create RAMDISK and load mount point
        _mount_ramdisks(server_vm)
        logging.info(server_vm.scratch_disks)
    else:
        # Detect Disks connected
        list_disks = detect_disks(server_vm)
        print(list_disks)
        sErrorMessage = _disk_mount_validations(list_disks)
        logging.info(f"message is {sErrorMessage}")
        if sErrorMessage != 'valid':
            raise errors.Setup.InvalidFlagConfigurationError(sErrorMessage)
        if len(FLAGS.ampere_disks) > 0 and set(FLAGS.ampere_disks).issubset(set(list_disks)):
            list_disks = FLAGS.ampere_disks
        if len(list_disks) > 0:
            _format_disk(server_vm, list_disks)
            size_disks = _sizeofdisks(server_vm,list_disks)
            partitions = _partition_disk(server_vm, list_disks, size_disks)
            _mount_file_system(server_vm, list_disks, partitions)
            logging.info(server_vm.scratch_disks)

def perform_disk_umount(server_vm: VirtualMachine):
    for disk_attached in server_vm.scratch_disks:
        if hasattr(disk_attached, "mount_point"):
            mountpoint = disk_attached.mount_point
            umount_cmd = (
                f"sleep 5;sudo umount {mountpoint};sleep 5;sudo rm -rf {mountpoint}"
            )
            server_vm.RemoteCommand(umount_cmd)

class DiskStatic:
    """Stores the mountpoint needed to create a disk."""
    mount_point: str
    metadata: dict

    def __init__(self, mountpoint, metadata_disk):
        self.mount_point = mountpoint
        self.metadata = metadata_disk

    def GetResourceMetadata(self):
        """Returns a dictionary of metadata about the resource."""
        return self.metadata
