# Modifications Copyright (c) 2024 Ampere Computing LLC
# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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

"""Installs/Configures Cassandra.

See 'perfkitbenchmarker/data/cassandra/' for configuration files used.

Cassandra homepage: http://cassandra.apache.org
"""

import posixpath
import time
from absl import flags
from six.moves import range

from perfkitbenchmarker import os_types
from ampere.pkb.common import download_utils

CASSANDRA_DIR = posixpath.join(download_utils.INSTALL_DIR, "cassandra")
CASSANDRA_PID = posixpath.join(CASSANDRA_DIR, "cassandra.pid")
CASSANDRA_OUT = posixpath.join(CASSANDRA_DIR, "cassandra.out")
CASSANDRA_ERR = posixpath.join(CASSANDRA_DIR, "cassandra.err")
NODETOOL = posixpath.join(CASSANDRA_DIR, "bin", "nodetool")

PACKAGE_NAME = "ampere_cassandra_server"
FLAGS = flags.FLAGS

CASSANDRA_HEAP_SIZE = flags.DEFINE_string(
    f"{PACKAGE_NAME}_heap_size",
    "31G",
    "Cassandra Heap size to calculate Garbage Collector",
)

flags.DEFINE_string(f"{PACKAGE_NAME}_version", "4.0.7", "Cassandra Version.")

flags.DEFINE_string(f"{PACKAGE_NAME}_jdk_version", "15.0.2", "Cassandra JDK Version")

flags.DEFINE_bool(
    f"{PACKAGE_NAME}_install_jdk_from_url",
    True,
    "install open JDK from url, "
    "if url is not present it will automatically gets installed from package",
)

flags.DEFINE_list("ampere_cassandra_server_mountpoints", ["nvme0n1"], "mount points")


flags.DEFINE_integer(
    f"{PACKAGE_NAME}_ObjectAlignmentInBytes", 32, "JVM option for ObjectAlignmentInByte"
)

flags.DEFINE_integer(
    f"{PACKAGE_NAME}_ParallelGCThreads", 48, "JVM option for ParallelGCThreads"
)

flags.DEFINE_bool(
    f"{PACKAGE_NAME}_use_numactl", False, "active numactl to run Cassandra"
)

flags.DEFINE_list(
    f"{PACKAGE_NAME}_use_cores", [], "add cores to specific instances of Cassandra"
)

flags.DEFINE_integer(f'{PACKAGE_NAME}_instances', 1,
                            'Concurrent cassandra instances.')

CASSANDRA_PORT = 9042
CASSANDRA_JMX_PORT = 7199
CASSANDRA_STORAGE_PORT = 7000


def GetJDKURL(arch) -> str:
    """Gets jdk url for passed architecture"""
    version = FLAGS[f"{PACKAGE_NAME}_jdk_version"].value
    if arch == "x86_64":
        arch = "x64"
    
    _JDK_HASH = {
        "15.0.2" : "0d1cfde4252546c6931946de8db48ee2/7",
        "17.0.2" : "dfd4a8d0985749f896bed50d7138ee7f/8",
    }
    
    url = f"https://download.java.net/java/GA/jdk{version}/{_JDK_HASH[version]}/GPL/openjdk-{version}_linux-{arch}_bin.tar.gz"

    return url


def _Install(vm):
    """Installs Cassandra from a tarball."""

    cassandra_version = FLAGS[f"{PACKAGE_NAME}_version"].value
    vm.InstallPackages("curl")
    vm.InstallPackages("wget")
    vm.Install("build_tools")
    vm.RemoteCommand(f"sudo rm -rf {CASSANDRA_DIR}")
    vm.RemoteCommand("sleep 2")
    # get Cassandra from user specific cassandra version.Replace cassandra version in url
    cassandra_url = (
        f"https://archive.apache.org/dist/cassandra/{cassandra_version}/"
        f"apache-cassandra-{cassandra_version}-bin.tar.gz"
    )
    cassandra_folder_tar = f"apache-cassandra-{cassandra_version}-bin.tar.gz"
    cassandra_folder = f"apache-cassandra-{cassandra_version}"
    vm.RemoteCommand(
        f"cd {download_utils.INSTALL_DIR} && wget --no-check-certificate {cassandra_url}"
    )
    vm.RemoteCommand(
        f"cd {download_utils.INSTALL_DIR} && tar -xzf {cassandra_folder_tar}"
    )
    vm.RemoteCommand(
        f"cd {download_utils.INSTALL_DIR} && mv {cassandra_folder} cassandra"
    )

    lscpu = vm.CheckLsCpu()
    arch = lscpu.data["Architecture"]
    if FLAGS[f"{PACKAGE_NAME}_install_jdk_from_url"].value:
        server_jdk_url = GetJDKURL(arch)
        vm.RemoteCommand(
            f"cd {download_utils.INSTALL_DIR} && wget --user-agent=Mozilla "
            f"--content-disposition -E -c {server_jdk_url} -O jdk.tar.gz"
        )
        vm.RemoteCommand(f"cd {download_utils.INSTALL_DIR} && tar -xzf jdk.tar.gz")
    else:
        vm.Install("openjdk")


def YumInstall(vm):
    """Installs Cassandra on the VM."""
    _Install(vm)


def AptInstall(vm):
    """Installs Cassandra on the VM."""
    _Install(vm)


def CreateInstances(vm):
    """Create user specified number of Cassandra Server Instances.
    Create copies of Cassandra instances on Server.
    Args:
        vm: Virtual Machine. The VM to condfigure
        no_of_instances: number of Cassandra Instances
    """
    for i in range(FLAGS[f"{PACKAGE_NAME}_instances"].value):
        i = i + 1
        folder_name = "cassandra_" + str(i)
        vm.RemoteCommand(f"cd {download_utils.INSTALL_DIR} && mkdir -p {folder_name}")
        vm.RemoteCommand(
            f"cd {download_utils.INSTALL_DIR} && cp -r cassandra/conf {folder_name}"
        )


def SedStrings(findstr, newstr, yaml_path, vm):
    """Replace string by finding strings"""
    command = f"sed -i 's|{findstr}|{newstr}|g' {yaml_path}"
    vm.RemoteCommand(command)


def CheckPortAvailable(vm, port):
    """Check if Port is  available on the system."""
    _, _, ret = vm.RemoteCommand(f"netstat -tulpn | grep LISTEN | grep :{port}")
    if ret == 0:
        raise ValueError(f"Port {port} is not available")


def _ConfigureYaml(vm, instance):
    """Configures Cassandra server YAML."""
    data_dir_name = ""
    disk_instance = instance * 2
    if len(vm.scratch_disks) == 0:
        if len(FLAGS.ampere_cassandra_server_mountpoints) > 1:
            if len(FLAGS.ampere_cassandra_server_mountpoints) == (
                FLAGS[f"{PACKAGE_NAME}_instances"].value * 2
            ):
                data_dir = FLAGS.ampere_cassandra_server_mountpoints[disk_instance]
                data_dir_name = posixpath.join(download_utils.INSTALL_DIR, data_dir)
            else:
                raise ValueError(
                    "Disk count and instances are not matching. Each Cassandra "
                    "instance needs 2 disks.Eg: 2 instances need 4 disks"
                )
        else:
            data_dir = "disk" + str(disk_instance)
            data_dir_name = posixpath.join(download_utils.INSTALL_DIR, data_dir)
    else:
        data_dir_name = vm.scratch_disks[disk_instance].mount_point
        vm.RemoteCommand(f"rm -rf {data_dir_name}/*")
    disk_instance = disk_instance + 1
    commitlog_dir_name = ""
    if len(vm.scratch_disks) == 0:
        if len(FLAGS.ampere_cassandra_server_mountpoints) > 1:
            if len(FLAGS.ampere_cassandra_server_mountpoints) == (
                FLAGS[f"{PACKAGE_NAME}_instances"].value * 2
            ):
                commitlog_dir = FLAGS.ampere_cassandra_server_mountpoints[disk_instance]
                commitlog_dir_name = posixpath.join(
                    download_utils.INSTALL_DIR, commitlog_dir
                )
            else:
                raise ValueError(
                    "Disk count and instances are not matching. Each Cassandra"
                    " instance needs 2 disks.Eg: 2 instances need 4 disks"
                )
        else:
            commitlog_dir = "disk" + str(disk_instance)
            commitlog_dir_name = posixpath.join(
                download_utils.INSTALL_DIR, commitlog_dir
            )
    else:
        commitlog_dir_name = vm.scratch_disks[disk_instance].mount_point
        vm.RemoteCommand(f"rm -rf {commitlog_dir_name}/*")
    # assign ports and check if port is available
    tmp_cassandra_storage_port = CASSANDRA_STORAGE_PORT + (instance * 2)
    ssl_cassandra_storage_port = CASSANDRA_STORAGE_PORT + (instance * 2) + 1
    tmp_cassandra_port = CASSANDRA_PORT + instance

    instance = instance + 1
    folder_name = "cassandra_" + str(instance)
    cassandra_dir = posixpath.join(download_utils.INSTALL_DIR, folder_name)
    yaml_path = posixpath.join(cassandra_dir, "conf/cassandra.yaml")
    cache_dir = posixpath.join(download_utils.INSTALL_DIR, "saved_caches")
    ip_address = vm.internal_ip + ":" + str(tmp_cassandra_storage_port)
    SedStrings("^# data_file_directories:*$", "data_file_directories:", yaml_path, vm)
    SedStrings(
        "^#     - /var/lib/cassandra/data", f"      - {data_dir_name}", yaml_path, vm
    )
    SedStrings(
        "^# commitlog_directory:.*$",
        f"commitlog_directory: {commitlog_dir_name}",
        yaml_path,
        vm,
    )
    SedStrings(
        "^# saved_caches_directory:.*$",
        f"saved_caches_directory: {cache_dir}",
        yaml_path,
        vm,
    )
    SedStrings("127.0.0.1:7000", f"{ip_address}", yaml_path, vm)
    SedStrings(
        "^storage_port:.*$",
        "storage_port: " + str(tmp_cassandra_storage_port) + "",
        yaml_path,
        vm,
    )
    vm.AllowPort(tmp_cassandra_storage_port)
    SedStrings(
        "^ssl_storage_port:.*$",
        "ssl_storage_port: " + str(ssl_cassandra_storage_port) + "",
        yaml_path,
        vm,
    )
    vm.AllowPort(ssl_cassandra_storage_port)
    SedStrings(
        "^listen_address:.*$", f"listen_address: {vm.internal_ip}", yaml_path, vm
    )
    SedStrings("^rpc_address:.*$", f"rpc_address: {vm.internal_ip}", yaml_path, vm)
    SedStrings(
        "^native_transport_port:.*$",
        f"native_transport_port: " + str(tmp_cassandra_port) + "",
        yaml_path,
        vm,
    )
    vm.AllowPort(tmp_cassandra_port)
    # Check if firewalld is installed on system by default
    stdout, stderr = vm.RemoteCommand(
            "sudo firewall-cmd --version", ignore_failure=True
            )
    if not stderr:
        vm.RemoteCommand(
                f"sudo firewall-cmd --zone=public --add-port={tmp_cassandra_storage_port}/tcp --permanent"
                )
        vm.RemoteCommand(
                f"sudo firewall-cmd --zone=public --add-port={ssl_cassandra_storage_port}/tcp --permanent"
                )
        vm.RemoteCommand(
                f"sudo firewall-cmd --zone=public --add-port={tmp_cassandra_port}/tcp --permanent"
                )
        vm.RemoteCommand(f"sudo firewall-cmd --reload")



def _ConfigureCassandraJVMOptions(vm, instance):
    instance = instance + 1
    folder_name = "cassandra_" + str(instance)
    cassandra_dir = posixpath.join(download_utils.INSTALL_DIR, folder_name)
    jvm_options_path = posixpath.join(cassandra_dir, "conf/jvm11-server.options")
    ObjectAlignmentInByte = FLAGS[f"{PACKAGE_NAME}_ObjectAlignmentInBytes"].value
    ParallelGCThreads = FLAGS[f"{PACKAGE_NAME}_ParallelGCThreads"].value
    SedStrings(
        "^-XX:+UseConcMarkSweepGC.*$", "#-XX:+UseConcMarkSweepGC", jvm_options_path, vm
    )
    SedStrings(
        "^-XX:+CMSParallelRemarkEnabled.*$",
        "#-XX:+CMSParallelRemarkEnabled",
        jvm_options_path,
        vm,
    )
    SedStrings("^-XX:SurvivorRatio=8.*$", "#-XX:SurvivorRatio=8", jvm_options_path, vm)
    SedStrings(
        "^-XX:MaxTenuringThreshold=1.*$",
        "#-XX:MaxTenuringThreshold=1",
        jvm_options_path,
        vm,
    )
    SedStrings(
        "^-XX:CMSInitiatingOccupancyFraction=75.*$",
        "#-XX:CMSInitiatingOccupancyFraction=75",
        jvm_options_path,
        vm,
    )
    SedStrings(
        "^-XX:+UseCMSInitiatingOccupancyOnly.*$",
        "#-XX:+UseCMSInitiatingOccupancyOnly",
        jvm_options_path,
        vm,
    )
    SedStrings(
        "^-XX:CMSWaitDuration=10000.*$",
        "#-XX:CMSWaitDuration=10000",
        jvm_options_path,
        vm,
    )
    SedStrings(
        "^-XX:+CMSParallelInitialMarkEnabled.*$",
        "#-XX:+CMSParallelInitialMarkEnabled",
        jvm_options_path,
        vm,
    )
    SedStrings(
        "^-XX:+CMSEdenChunksRecordAlways.*$",
        "#-XX:+CMSEdenChunksRecordAlways",
        jvm_options_path,
        vm,
    )
    SedStrings(
        "^-XX:+CMSClassUnloadingEnabled.*$",
        "#-XX:+CMSClassUnloadingEnabled",
        jvm_options_path,
        vm,
    )
    # replace_str11 = rf"s|^#-XX:+ParallelRefProcEnabled.*$|-XX:+UseParallelGC\n-XX:+ParallelRefProcEnabled\n-XX:+UseCompressedOops\n-XX:ObjectAlignmentInBytes={ObjectAlignmentInByte}|g"

    SedStrings(
        "^#-XX:+ParallelRefProcEnabled.*$",
        "-XX:+UseParallelGC\\n-XX:+ParallelRefProcEnabled\\n-XX:"
        f"+UseCompressedOops\\n-XX:ObjectAlignmentInBytes={ObjectAlignmentInByte}",
        jvm_options_path,
        vm,
    )
    # vm.RemoteCommand(f"sed -i '{replace_str11}' {jvm_options_path} ")
    SedStrings(
        "^#-XX:ParallelGCThreads=16",
        f"-XX:ParallelGCThreads={ParallelGCThreads}",
        jvm_options_path,
        vm,
    )


def _ConfigureCassandraENVOptions(vm, instance):
    tmp_cassandra_jmx_port = CASSANDRA_JMX_PORT + instance
    instance = instance + 1
    folder_name = "cassandra_" + str(instance)
    cassandra_dir = posixpath.join(download_utils.INSTALL_DIR, folder_name)
    cassandra_env_path = posixpath.join(cassandra_dir, "conf/cassandra-env.sh")
    replace_env_str1 = rf"s/  *LOCAL_JMX=yes\nfi/    LOCAL_JMX=yes\nfi\nLOCAL_JMX=no/g"
    vm.RemoteCommand(f"sed -zi '{replace_env_str1}' {cassandra_env_path} ")
    SedStrings(
        "^JMX_PORT=.*$",
        'JMX_PORT="' + str(tmp_cassandra_jmx_port) + '"',
        cassandra_env_path,
        vm,
    )
    SedStrings(
        "#MAX_HEAP_SIZE=.*$",
        'MAX_HEAP_SIZE="' + CASSANDRA_HEAP_SIZE.value + '"',
        cassandra_env_path,
        vm,
    )
    SedStrings("#HEAP_NEWSIZE=.*$", 'HEAP_NEWSIZE="12288M"', cassandra_env_path, vm)
    # replace_env_str6 = rf's|# JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.authenticate=true"| JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.authenticate=true"|g'
    # vm.RemoteCommand(f"sed -i '{replace_env_str6}' {cassandra_env_path} ")
    SedStrings(
        '# JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.authenticate=true"',
        ' JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.authenticate=true"',
        cassandra_env_path,
        vm,
    )
    replace_str12 = 'JVM_OPTS="$JVM_OPTS -Xss512k"'
    vm.RemoteCommand(f"sed -i -e '$a{replace_str12}' {cassandra_env_path} ")
    replace_str13 = (
        'JVM_OPTS="$JVM_OPTS -Dcassandra.max_queued_native_transport_requests=4096"'
    )
    vm.RemoteCommand(f"sed -i -e '$a{replace_str13}' {cassandra_env_path} ")


def Configure(vm):
    """Configure Cassandra on 'vm'.

    Args:
      vm: VirtualMachine. The VM to configure.
      CASSANDRA_HEAP_SIZE: update heap size in cassndra-env.sh on Cassandra Server
      no_of_instances: number of Cassandra Instances
    """
    if len(vm.scratch_disks) == 0:
        if len(FLAGS.ampere_cassandra_server_mountpoints) == 0:
            totaldisks = FLAGS[f"{PACKAGE_NAME}_instances"].value * 2
            for disk in range(totaldisks):
                vm.RemoteCommand(f"mkdir {download_utils.INSTALL_DIR}/disk{disk}")
    for instance in range(FLAGS[f"{PACKAGE_NAME}_instances"].value):
        _ConfigureYaml(vm, instance)
        _ConfigureCassandraJVMOptions(vm, instance)
        _ConfigureCassandraENVOptions(vm, instance)


def Start(vm, instance_no):
    """Start the Server"""
    jdk_version = FLAGS[f"{PACKAGE_NAME}_jdk_version"].value
    instance = instance_no + 1
    folder_name = "cassandra_" + str(instance)
    cassandra_conf_dir = posixpath.join(download_utils.INSTALL_DIR, folder_name, "conf")
    cassandra_dir = posixpath.join(download_utils.INSTALL_DIR, "cassandra")
    cassandra_str = ""
    if FLAGS[f"{PACKAGE_NAME}_use_numactl"].value:
        cassandra_str = (
            f"export JAVA_HOME={download_utils.INSTALL_DIR}/jdk-{jdk_version}/ && "
            f" export PATH=$PATH:{download_utils.INSTALL_DIR}/jdk-{jdk_version}/bin &&"
            f" export CASSANDRA_CONF={cassandra_conf_dir} && "
            f" numactl -C {FLAGS.ampere_cassandra_server_use_cores[instance_no]} "
            f"{cassandra_dir}/bin/cassandra & "
        )
    else:
        cassandra_str = (
            f"export JAVA_HOME={download_utils.INSTALL_DIR}/jdk-{jdk_version}/ &&"
            f"export PATH=$PATH:{download_utils.INSTALL_DIR}/jdk-{jdk_version}/bin &&"
            f" export CASSANDRA_CONF={cassandra_conf_dir} && {cassandra_dir}/bin/cassandra & "
        )
    return cassandra_str


def Stop(vm):
    """Stops Cassandra on 'vm'."""
    vm.RemoteCommand("sudo pkill -9 java", ignore_failure=True)
    time.sleep(10)


def CleanNode(vm, no_of_instances):
    """Remove Cassandra data from 'vm'.

    Args:
      vm: VirtualMachine. VM to clean.
      no_of_instances: Number of Instances spawned
    """
    for i in range(no_of_instances):
        disk_instance = i * 2
        instance = i + 1
        folder_name = "cassandra_" + str(instance)
        cassandra_dir = posixpath.join(download_utils.INSTALL_DIR, folder_name)
        if len(vm.scratch_disks) == 0:
            if len(FLAGS.ampere_cassandra_server_mountpoints) > 1:
                if len(FLAGS.ampere_cassandra_server_mountpoints) == (
                    no_of_instances * 2
                ):
                    data_dir = FLAGS.ampere_cassandra_server_mountpoints[disk_instance]
                    data_dir_name = posixpath.join(download_utils.INSTALL_DIR, data_dir)
            else:
                data_dir = "disk" + str(disk_instance)
                data_dir_name = posixpath.join(download_utils.INSTALL_DIR, data_dir)
        else:
            data_dir_name = vm.scratch_disks[disk_instance].mount_point
        vm.RemoteCommand(f"rm -rf {data_dir_name}/*")
        vm.RemoteCommand(f"sudo umount {data_dir_name}")
        instance = instance + 1
        disk_instance = disk_instance + 1
        commitlog_dir_name = ""
        if len(vm.scratch_disks) == 0:
            if len(FLAGS.ampere_cassandra_server_mountpoints) > 1:
                if len(FLAGS.ampere_cassandra_server_mountpoints) == (
                    FLAGS[f"{PACKAGE_NAME}_instances"].value * 2
                ):
                    commitlog_dir = FLAGS.ampere_cassandra_server_mountpoints[
                        disk_instance
                    ]
                    commitlog_dir_name = posixpath.join(
                        download_utils.INSTALL_DIR, commitlog_dir
                    )
            else:
                commitlog_dir = "disk" + str(disk_instance)
                commitlog_dir_name = posixpath.join(
                    download_utils.INSTALL_DIR, commitlog_dir
                )
        else:
            commitlog_dir_name = vm.scratch_disks[disk_instance].mount_point
        vm.RemoteCommand(f"rm -rf {commitlog_dir_name}/*")
        vm.RemoteCommand(f"sudo umount {commitlog_dir_name}")
        vm.RemoteCommand(f"sudo rm -rf {cassandra_dir}")
    vm.RemoteCommand(f"sudo rm -rf {download_utils.INSTALL_DIR}")


def GetCassandraCqlshPath():
    """Get Cassandra cql Path"""
    return posixpath.join(CASSANDRA_DIR, "bin", "cqlsh")
