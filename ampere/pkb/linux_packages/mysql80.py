# Modifications Copyright (c) 2024 Ampere Computing LLC
# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing mysql installation and cleanup functions."""

import posixpath
import time
from absl import flags
from six.moves import range

from perfkitbenchmarker import data
from ampere.pkb.common import download_utils

PACKAGE_NAME = "ampere_mysql"
BENCHMARK_NAME = "ampere_mysql_sysbench"

FLAGS = flags.FLAGS

MYSQL_CNF_TEMPLATE = "mysql/my.cnf"

flags.DEFINE_string(f"{PACKAGE_NAME}_version_number", "8.0.36", "Mysql Version Number.")

flags.DEFINE_string(
    f"{PACKAGE_NAME}_libtirpc_version_number", "1.3.4", "Libtirpc Version Number."
)

flags.DEFINE_string(f"{PACKAGE_NAME}_version", "8.0", "Mysql Version.")

COMPILE_TYPE = flags.DEFINE_string(
    f"{PACKAGE_NAME}_compile_type", "user_defined", "OPT Flag value"
)

COMPILE_OPT_FLAG = flags.DEFINE_string(
    f"{PACKAGE_NAME}_compile_opt_flag",
    "-g -O3 -fno-omit-frame-pointer -march=armv8.2-a -DNDEBUG",
    "OPT Flag value",
)

MYSQL_BUILD_TYPE = flags.DEFINE_string(
    f"{PACKAGE_NAME}_build_type", "Release", "Mysql build type"
)
UNIT_TEST_VALUE = flags.DEFINE_string(
    f"{PACKAGE_NAME}_unit_test", "OFF", "Mysql unit test Defaults to OFF"
)

flags.DEFINE_string(
    f"{PACKAGE_NAME}_install_dir", "mysql", "Mysql installation directory"
)

MYSQL_DATA_DIR = flags.DEFINE_string(
    f"{PACKAGE_NAME}_data_dir", "mysql_benchmark", "Mysql data directory"
)


flags.DEFINE_integer(
    f"{PACKAGE_NAME}_instances", "1", "Mysql server instances Default to 1"
)


flags.DEFINE_bool(f"{PACKAGE_NAME}_use_numactl", False, "active numactl to run MySQL")

flags.DEFINE_list(
    f"{PACKAGE_NAME}_use_cores", [], "add cores to specific instances of MySQL"
)

flags.DEFINE_string(f"{PACKAGE_NAME}_pgo_status", "create", "check pgo status")

flags.DEFINE_string(
    f"{PACKAGE_NAME}_innodb_buffer_pool_size",
    "64G",
    "set innodb buffer pool size in my.cnf",
)

flags.DEFINE_integer(
    f"{PACKAGE_NAME}_innodb_write_io_threads",
    "64",
    "set innodb write io threads in my.cnf",
)

flags.DEFINE_integer(
    f"{PACKAGE_NAME}_innodb_read_io_threads",
    "64",
    "set innodb read io threads in my.cnf",
)

flags.DEFINE_integer(
    f"{PACKAGE_NAME}_max_connections",
    "10000",
    "set max connections in my.cnf",
)

flags.DEFINE_integer(
    f"{PACKAGE_NAME}_max_user_connections",
    "2100",
    "set max user connections in my.cnf",
)

flags.DEFINE_integer(
    f"{PACKAGE_NAME}_innodb_buffer_pool_instances",
    "80",
    "set innodb buffer pool instances in my.cnf",
)

flags.DEFINE_integer(
    f"{PACKAGE_NAME}_innodb_thread_concurrency",
    "128",
    "set innodb thread concurrency in my.cnf",
)

flags.DEFINE_string(
    f"{PACKAGE_NAME}_innodb_redo_log_capacity",
    "20G",
    "set innodb redo log capacity  in my.cnf",
)


flags.DEFINE_list(f"{PACKAGE_NAME}_mountpoints", ["nvme0n1"], "mount points")

MYSQL_DATA = flags.DEFINE_string(
    f"{PACKAGE_NAME}_data_conf",
    None,
    "An alternate my.cnf Must be located in ./ampere/pkb/data/ ",
)


MYSQL_PORT = flags.DEFINE_integer(
    f"{PACKAGE_NAME}_port", "3000", "Mysql server port Default to 3000"
)

BENCHMARK_NAME = "ampere_mysql_sysbench"
MYSQL_PASSWORD = "123456"


def CheckPortAvailable(vm, port):
    """Check if Port is  available on the system."""
    out, err, ret = vm.RemoteCommand(
        f"sudo netstat -tulpn | grep LISTEN | grep :{port}", ignore_failure=True
    )
    if out == 0:
        raise ValueError(f"Port {port} is not available")


def _Install(vm):
    """Install Mysql from tarball"""
    mysql_version_number = FLAGS[f"{PACKAGE_NAME}_version_number"].value
    mysql_url = (
        f"https://github.com/mysql/mysql-server/archive/refs/tags/mysql-"
        f"{mysql_version_number}.tar.gz"
    )
    mysql_folder_tar = f"mysql-{mysql_version_number}.tar.gz"
    vm.RemoteCommand(
        f"cd {download_utils.INSTALL_DIR} && sudo wget --no-check-certificate {mysql_url}"
    )
    vm.RemoteCommand(f"cd {download_utils.INSTALL_DIR} && tar -xzf {mysql_folder_tar}")
    vm.RemoteCommand(f"cd {download_utils.INSTALL_DIR} && mkdir -p build;")
    mysql_data_directory = posixpath.join(
        download_utils.INSTALL_DIR, f"{MYSQL_DATA_DIR.value}"
    )
    vm.RemoteCommand(f"mkdir -p {mysql_data_directory}")
    port = MYSQL_PORT.value
    vm.AllowPort(port)
    # Check if firewalld is installed on system by default
    stdout, stderr = vm.RemoteCommand(
            "sudo firewall-cmd --version", ignore_failure=True
            )
    if not stderr:
        vm.RemoteCommand(
                f"sudo firewall-cmd --zone=public --add-port={port}/tcp --permanent"
                )
        vm.RemoteCommand(f"sudo firewall-cmd --reload")

    data_temp = "data" + str(port)
    dbs_temp = "dbs" + str(port)
    mysql_basedir = posixpath.join(f"{mysql_data_directory}", f"{data_temp}")
    mysql_datadir = posixpath.join(
        f"{mysql_data_directory}", f"{data_temp}", f"{dbs_temp}"
    )
    mysql_tmpdir = posixpath.join(f"{mysql_data_directory}", f"{data_temp}", "tmp")
    vm.RemoteCommand(f"mkdir -p {mysql_basedir} {mysql_datadir} {mysql_tmpdir}")


def MysqlBuild(vm):
    """Install Mysql from tarball"""
    # Install libtirpc to build mysql
    libtirpc_version_number = FLAGS[f"{PACKAGE_NAME}_libtirpc_version_number"].value
    libtirpc_url = (
        f"https://downloads.sourceforge.net/libtirpc/"
        f"libtirpc-{libtirpc_version_number}.tar.bz2"
    )
    libtirpc_folder_tar = f"libtirpc-{libtirpc_version_number}.tar.bz2"
    vm.RemoteCommand(
        f"cd {download_utils.INSTALL_DIR} && sudo wget --no-check-certificate {libtirpc_url}"
    )
    vm.RemoteCommand(
        f"cd {download_utils.INSTALL_DIR} && tar -xvjf {libtirpc_folder_tar}"
    )
    libtirpc_folder = f"libtirpc-{libtirpc_version_number}"
    vm.RemoteCommand(f"cd {download_utils.INSTALL_DIR} && mkdir -p libtirpc;")
    libtirpc_download_dir = posixpath.join(
        download_utils.INSTALL_DIR, f"{libtirpc_folder}"
    )
    libtirpc_install_dir = posixpath.join(download_utils.INSTALL_DIR, "libtirpc")
    vm.RemoteCommand(
        f"cd {libtirpc_download_dir} && ./configure --prefix={libtirpc_install_dir}"
    )
    vm.RemoteCommand(f"cd {libtirpc_download_dir} && make -j `nproc`")
    vm.RemoteCommand(f"cd {libtirpc_download_dir} && sudo make install")
    pkg_config_path = (
        f"export PKG_CONFIG_PATH={libtirpc_install_dir}/lib/pkgconfig:$PKG_CONFIG_PATH"
    )
    # Install openssl
    if FLAGS["ampere_openssl_use"].value:
        user_check, _ = vm.RemoteCommand("whoami")
        user_check = user_check.strip()
        vm.Install("ampere_openssl")
        vm.RemoteCommand(f"sed -i '/export/d' /home/{user_check}/.bashrc")
        vm.RemoteCommand(
            f" echo 'export PATH="
            f"{download_utils.INSTALL_DIR}/openssl/bin:$PATH' >> /home/{user_check}/.bashrc"
        )
    mysql_version_number = FLAGS[f"{PACKAGE_NAME}_version_number"].value
    mysql_folder = f"mysql-server-mysql-{mysql_version_number}"
    build_path = posixpath.join(download_utils.INSTALL_DIR, "build")
    mysql_path = posixpath.join(download_utils.INSTALL_DIR, f"{mysql_folder}")
    mysql_install_dir = FLAGS[f"{PACKAGE_NAME}_install_dir"].value
    mysql_install_path = posixpath.join(
        download_utils.INSTALL_DIR, f"{mysql_install_dir}"
    )
    compile_flag_value = ""
    if COMPILE_TYPE.value == "user_defined":
        compile_flag_value = COMPILE_OPT_FLAG.value
    else:
        lscpu = vm.CheckLsCpu()
        arch = lscpu.data["Architecture"]
        if arch == "x86_64":
            compile_flag_value = "-O3 -fno-omit-frame-pointer -march=native "
        else:
            compile_flag_value = "-O3 -fno-omit-frame-pointer -march=armv8.2-a "
    if FLAGS[f"{BENCHMARK_NAME}_pgo"].value:
        if FLAGS[f"{PACKAGE_NAME}_pgo_status"].value == "create":
            vm.RemoteCommand("mkdir -p /tmp/pgo_dir")
            compile_flag_value = (
                compile_flag_value + " -fprofile-generate -fprofile-dir=/tmp/pgo_dir "
            )
            FLAGS[f"{PACKAGE_NAME}_pgo_status"].value = "user"
        else:
            vm.RemoteCommand(f"cd {download_utils.INSTALL_DIR} && mkdir -p pgo_build;")
            build_path = posixpath.join(download_utils.INSTALL_DIR, "pgo_build")
            compile_flag_value = (
                compile_flag_value
                + " -fprofile-use -fprofile-dir=/tmp/pgo_dir "
                  "-fprofile-correction -Wno-error=missing-profile "
            )
    compile_flag_value = compile_flag_value + f"-L{libtirpc_install_dir}/lib -ltirpc"
    os_type, _ = vm.RemoteCommand('cat /etc/os-release  | grep ^ID= | cut -d "=" -f2')
    os_type = os_type.strip()
    check_openssl = ""
    if FLAGS["ampere_openssl_use"].value:
        check_openssl = f"-DWITH_SSL={download_utils.INSTALL_DIR}/openssl"
    else:
        vm.InstallPackages("openssl-devel")
    gcc_value = ""
    if "centos" in os_type:
        vm.RemoteCommand(
            f"cd /opt/rh/gcc-toolset-10/root/usr/lib/gcc/{arch}-redhat-linux/10/plugin "
            f"&& sudo ln -s annobin.so gcc-annobin.so"
        )
        gcc_value = ("export CC=/opt/rh/gcc-toolset-10/root/usr/bin/gcc "
                     "&& export CXX=/opt/rh/gcc-toolset-10/root/usr/bin/g++ && ")
    # Get boost manually to avoid flaky downloads in cmake 
    #   - boost_1_77_0 is compatible with MySQL 8.0.36
    boost_basename = "boost_1_77_0"
    boost_url = f"https://archives.boost.io/release/1.77.0/source/{boost_basename}.tar.bz2"
    boost_path = posixpath.join(download_utils.INSTALL_DIR, boost_basename)
    vm.RemoteCommand(f"wget {boost_url} -P {download_utils.INSTALL_DIR}")
    vm.RemoteCommand(f"tar -xvf {boost_path}.tar.bz2 -C {download_utils.INSTALL_DIR}")
    # Build MySQL (include boost from previous step)
    vm.RemoteCommand(
        f"cd {build_path} && {gcc_value} {pkg_config_path} && "
        f"export LD_LIBRARY_PATH={libtirpc_install_dir}/lib && "
        f"cmake {mysql_path} "
        f'-DWITH_BOOST={boost_path} -DCMAKE_C_FLAGS="{compile_flag_value}"'
        f' -DCMAKE_CXX_FLAGS="{compile_flag_value}" '
        f"-DCMAKE_BUILD_TYPE={MYSQL_BUILD_TYPE.value}  "
        f"-DWITH_UNIT_TESTS={UNIT_TEST_VALUE.value} "
        f"-DCMAKE_INSTALL_PREFIX={mysql_install_path} {check_openssl}"
        f" -DRPC_INCLUDE_DIRS={libtirpc_install_dir}/include/tirpc"
    )
    time.sleep(10)
    vm.RemoteCommand(
        f"cd {build_path} && "
        f"export LD_LIBRARY_PATH={libtirpc_install_dir}/lib && make -j `nproc`"
    )
    vm.RemoteCommand(f"cd {build_path} && sudo make install")


def RemoveBuild(vm):
    count = 0
    mysql_data_directory = posixpath.join(
        download_utils.INSTALL_DIR, f"{MYSQL_DATA_DIR.value}"
    )
    data_temp = "data" + str(MYSQL_PORT.value)
    mysql_tmpdir = posixpath.join(f"{mysql_data_directory}", f"{data_temp}", "tmp")
    mysql_install_dir = FLAGS[f"{PACKAGE_NAME}_install_dir"].value
    mysql_install_path = posixpath.join(
        download_utils.INSTALL_DIR, f"{mysql_install_dir}"
    )
    libtirpc_install_dir = posixpath.join(download_utils.INSTALL_DIR, "libtirpc")
    vm.RemoteCommand(
        f"export LD_LIBRARY_PATH={libtirpc_install_dir}/lib && "
        f"sudo {mysql_install_path}/bin/mysqladmin "
        f"--socket={mysql_tmpdir}/mysql.sock -uroot -p{MYSQL_PASSWORD} shutdown"
    )
    build_path = posixpath.join(download_utils.INSTALL_DIR, "build")
    vm.RemoteCommand(f"cd {build_path} && make clean")
    time.sleep(25)
    vm.RemoteCommand(f"cd {build_path} && sudo rm -rf CMakeCache.txt")
    data_dir_name = ""
    if len(vm.scratch_disks) == 0:
        if len(FLAGS.ampere_mysql_mountpoints) != 0:
            data_dir = FLAGS.ampere_mysql_mountpoints[0]
            data_dir_name = posixpath.join(download_utils.INSTALL_DIR, data_dir)
        else:
            data_dir = "disk" + str(count)
            data_dir_name = posixpath.join(download_utils.INSTALL_DIR, data_dir)
    else:
        data_dir_name = vm.scratch_disks[count].mount_point
    vm.RemoteCommand(f"cd {data_dir_name} && sudo rm -rf *")
    time.sleep(25)


def YumInstall(vm):
    """Installs the mysql package on the VM."""
    vm.InstallPackages("curl")
    vm.InstallPackages("numactl")
    vm.InstallPackages("wget")
    vm.InstallPackages("perl")
    vm.InstallPackages(
        "make automake libtool pkgconfig libaio-devel git curl wget "
        "postgresql-devel libzstd-devel  zlib-devel perl krb5-devel"
    )
    vm.Install("build_tools")
    lscpu = vm.CheckLsCpu()
    arch = lscpu.data["Architecture"]
    os_type, _ = vm.RemoteCommand('cat /etc/os-release  | grep ^ID= | cut -d "=" -f2')
    os_type = os_type.strip()
    # Depending on OS Type handling of packages for installation differs
    # Hence checking what OS is present whether Centos or Oracle Linux
    check_patchelf = "sudo yum list installed | grep patchelf"
    stdout, stderr, ret_code = vm.RemoteCommandWithReturnCode(
        check_patchelf, ignore_failure=True
    )
    if ret_code == 1:
        vm.RemoteCommand(
            f"cd {download_utils.INSTALL_DIR} && "
            f"sudo wget https://www.rpmfind.net/linux/mageia/distrib/8/{arch}/"
            f"media/core/updates/patchelf-0.16.1-1.mga8.{arch}.rpm"
        )
        vm.RemoteCommand(
            f"cd {download_utils.INSTALL_DIR} && "
            f"sudo rpm -i patchelf-0.16.1-1.mga8.{arch}.rpm",
            ignore_failure=True,
        )
    if "centos" in os_type:
        vm.InstallPackages(
            "pkg-config cmake bison gcc-toolset-10"
            " gcc-toolset-10-gcc gcc-toolset-10-gcc-c++ gcc-toolset-10-binutils"
            " ncurses-devel rpcgen"
        )
        vm.RemoteCommand(
            "export LD_LIBRARY_PATH=/opt/rh/gcc-toolset-10:$LD_LIBRARY_PATH"
        )
        vm.RemoteCommand("export PATH=/opt/rh/gcc-toolset-10:$PATH")
    elif "ol" in os_type or "rhel" in os_type:
        vm.InstallPackages(
            "pkg-config cmake bison gcc g++ ncurses-devel  rpcgen gcc-toolset-12-gcc "
            "gcc-toolset-12-gcc-c++ "
            "gcc-toolset-12-binutils gcc-toolset-12-annobin-annocheck "
            "gcc-toolset-12-annobin-plugin-gcc"
        )
    elif "almalinux" in os_type:
        vm.InstallPackages(
            "pkg-config cmake bison ncurses-devel rpcgen "
            "gcc-toolset-12-gcc gcc-toolset-12-gcc-c++ gcc-toolset-12-binutils "
            "gcc-toolset-12-annobin-annocheck gcc-toolset-12-annobin-plugin-gcc "
            "libudev-devel libtirpc-devel"
        )
    else:
        vm.InstallPackages(
            "pkg-config gcc g++  cmake bison ncurses-devel libudev-devel "
            "libtirpc rpcgen  libtirpc-devel"
        )
    _Install(vm)


def AptInstall(vm):
    """Installs the mysql package on the VM."""
    vm.Install("build_tools")
    vm.InstallPackages("numactl")
    vm.InstallPackages(
        "curl wget pkg-config gcc g++  cmake libssl-dev libntirpc-dev "
        "libudev-dev bison libncurses5-dev libtirpc-dev net-tools patchelf libkrb5-dev"
    )
    vm.RemoteCommand("sudo apt autoremove -y")
    _Install(vm)


def Configure(vm):
    """Configure Mysql on 'vm'.

    Args:
      vm: VirtualMachine. The VM to configure.
      seed_vms: List of VirtualMachine. The seed virtual machine(s).
      no_of_instances: number of Mysql Instances
    """
    mysql_data_directory = posixpath.join(
        download_utils.INSTALL_DIR, f"{MYSQL_DATA_DIR.value}"
    )
    count = 0
    for instance in range(FLAGS[f"{PACKAGE_NAME}_instances"].value):
        data_dir_name = ""
        if len(vm.scratch_disks) == 0:
            if len(FLAGS.ampere_mysql_mountpoints) != 0:
                data_dir = FLAGS.ampere_mysql_mountpoints[0]
                data_dir_name = posixpath.join(download_utils.INSTALL_DIR, data_dir)
            else:
                data_dir = "disk" + str(count)
                data_dir_name = posixpath.join(download_utils.INSTALL_DIR, data_dir)
        else:
            data_dir_name = vm.scratch_disks[count].mount_point
        count = count + 1
        time.sleep(10)
        port = MYSQL_PORT.value + instance
        vm.AllowPort(port)
        data_temp = "data" + str(port)
        mysql_basedir = posixpath.join(f"{mysql_data_directory}", f"{data_temp}")
        mysql_tmpdir = posixpath.join(f"{mysql_data_directory}", f"{data_temp}", "tmp")
        mysql_conf_path = posixpath.join(f"{mysql_basedir}", "my.cnf")
        file_path = data.ResourcePath(MYSQL_DATA.value)
        vm.RemoteCopy(file_path, mysql_conf_path)
        buffer_size = FLAGS[f"{PACKAGE_NAME}_innodb_buffer_pool_size"].value
        read_io = FLAGS[f"{PACKAGE_NAME}_innodb_read_io_threads"].value
        write_io = FLAGS[f"{PACKAGE_NAME}_innodb_write_io_threads"].value
        max_connection = FLAGS[f"{PACKAGE_NAME}_max_connections"].value
        max_user_connections = FLAGS[f"{PACKAGE_NAME}_max_user_connections"].value
        innodb_buffer_pool_instances = FLAGS[
            f"{PACKAGE_NAME}_innodb_buffer_pool_instances"
        ].value
        innodb_thread_concurrency = FLAGS[
            f"{PACKAGE_NAME}_innodb_thread_concurrency"
        ].value
        innodb_redo_log_capacity = FLAGS[
            f"{PACKAGE_NAME}_innodb_redo_log_capacity"
        ].value
        replacements = [
            rf"s|%PORT%|{port}|g",
            rf"s|%DATA_ROOT%|{mysql_data_directory}|g",
            rf"s|%DATA_ROOT_DIR%|{data_dir_name}|g",
            rf"s|innodb_buffer_pool_size=64G|innodb_buffer_pool_size={buffer_size}|g",
            rf"s|innodb_read_io_threads=64|innodb_read_io_threads={read_io}|g",
            rf"s|innodb_write_io_threads=64|innodb_write_io_threads={write_io}|g",
            rf"s|max_connections=10000|max_connections={max_connection}|g",
            rf"s|max_user_connections=2100|max_user_connections={max_user_connections}|g",
            rf"s|innodb_buffer_pool_instances=80|innodb_buffer_pool_instances={innodb_buffer_pool_instances}|g",
            rf"s|innodb_thread_concurrency=128|innodb_thread_concurrency={innodb_thread_concurrency}|g",
            rf"s|innodb_redo_log_capacity=20G|innodb_redo_log_capacity={innodb_redo_log_capacity}|g",
        ]
        for replacement in replacements:
            vm.RemoteCommand(f"sudo sed -i '{replacement}' {mysql_conf_path}")
        time.sleep(10)
        vm.RemoteCommand(f"chmod 644 {mysql_conf_path}")
        libtirpc_install_dir = posixpath.join(download_utils.INSTALL_DIR, "libtirpc")
        mysql_install_dir = FLAGS[f"{PACKAGE_NAME}_install_dir"].value
        mysql_install_path = posixpath.join(
            download_utils.INSTALL_DIR, f"{mysql_install_dir}"
        )
        if FLAGS[f"{PACKAGE_NAME}_use_numactl"].value:
            numa_prefix = f"numactl -C {FLAGS.ampere_mysql_use_cores[0]}"
        else:
            numa_prefix = ""
        vm.RemoteCommand(
            f"export LD_LIBRARY_PATH={libtirpc_install_dir}/lib && "
            f"{numa_prefix} {mysql_install_path}/bin/mysqld --defaults-file={mysql_conf_path} "
            f"--skip-grant-tables --user=root --initialize >> "
            f"{mysql_basedir}/mysql_install_db.log 2>&1"
        )
        time.sleep(25)
        vm.RemoteCommand(
            f"export LD_LIBRARY_PATH={libtirpc_install_dir}/lib && "
            f"{numa_prefix} {mysql_install_path}/bin/mysqld --defaults-file={mysql_conf_path}"
            f" --user=root -D --bind-address=0.0.0.0"
        )
        time.sleep(25)

        mysql_client = f"{mysql_install_path}/bin/mysql"
        password_line, _ = vm.RemoteCommand(
            f'grep "A temporary password is generated" {mysql_tmpdir}/error.log'
        )
        password_line = password_line.strip()
        real_password = password_line.split(": ")
        old_password = real_password[-1]
        vm.RemoteCommand(
            f"export LD_LIBRARY_PATH={libtirpc_install_dir}/lib && {mysql_client} "
            f"--socket={mysql_tmpdir}/mysql.sock --connect-expired-password "
            f"-uroot -p\"{old_password}\" -e \"ALTER USER 'root'@'localhost' IDENTIFIED BY "
            f"'{MYSQL_PASSWORD}';\""
        )
        vm.RemoteCommand(
            f"export LD_LIBRARY_PATH={libtirpc_install_dir}/lib && {mysql_client} "
            f'--socket={mysql_tmpdir}/mysql.sock -uroot -p"{MYSQL_PASSWORD}"'
            f" -e \"GRANT ALL ON *.* TO 'root'@'localhost';\""
        )
        vm.RemoteCommand(
            f"export LD_LIBRARY_PATH={libtirpc_install_dir}/lib && {mysql_client} "
            f'--socket={mysql_tmpdir}/mysql.sock -uroot -p"{MYSQL_PASSWORD}" '
            f'-e "create database sbtest"'
        )
        vm.RemoteCommand(
            f"export LD_LIBRARY_PATH={libtirpc_install_dir}/lib && {mysql_client} "
            f"--socket={mysql_tmpdir}/mysql.sock "
            f'-uroot -p"{MYSQL_PASSWORD}" '
            f"-e \"CREATE USER 'sbtest'@'%' IDENTIFIED WITH "
            f"mysql_native_password BY '{MYSQL_PASSWORD}';\""
        )
        vm.RemoteCommand(
            f"export LD_LIBRARY_PATH={libtirpc_install_dir}/lib && {mysql_client} "
            f'--socket={mysql_tmpdir}/mysql.sock -uroot -p"{MYSQL_PASSWORD}"'
            f" -e \"GRANT ALL ON *.* TO 'sbtest'@'%';\""
        )


def CleanNode(vm):
    """Remove Mysql data from 'vm'.

    Args:
      vm: VirtualMachine. VM to clean.
    """
    mysql_data_directory = posixpath.join(
        download_utils.INSTALL_DIR, f"{MYSQL_DATA_DIR.value}"
    )
    data_temp = "data" + str(MYSQL_PORT.value)
    mysql_tmpdir = posixpath.join(f"{mysql_data_directory}", f"{data_temp}", "tmp")
    mysql_install_dir = FLAGS[f"{PACKAGE_NAME}_install_dir"].value
    mysql_install_path = posixpath.join(
        download_utils.INSTALL_DIR, f"{mysql_install_dir}"
    )
    libtirpc_install_dir = posixpath.join(download_utils.INSTALL_DIR, "libtirpc")
    vm.RemoteCommand(
        f"export LD_LIBRARY_PATH={libtirpc_install_dir}/lib && "
        f"sudo {mysql_install_path}/bin/mysqladmin --socket={mysql_tmpdir}/mysql.sock "
        f"-uroot -p{MYSQL_PASSWORD} shutdown"
    )
    time.sleep(10)
    if len(FLAGS.ampere_mysql_mountpoints) == 0:
        vm.RemoteCommand(f"sudo umount {download_utils.INSTALL_DIR}/disk")
    if len(vm.scratch_disks) == 0:
        if len(FLAGS.ampere_mysql_mountpoints) != 0:
            data_dir = FLAGS.ampere_mysql_mountpoints[0]
            data_dir_name = posixpath.join(download_utils.INSTALL_DIR, data_dir)
        else:
            data_dir = "disk"
            data_dir_name = posixpath.join(download_utils.INSTALL_DIR, data_dir)
    else:
        data_dir_name = vm.scratch_disks[0].mount_point
    vm.RemoteCommand(f"sudo rm -rf {data_dir_name}/*")
    time.sleep(90)
    vm.RemoteCommand(f"sudo umount {data_dir_name}")
    time.sleep(20)
    vm.RemoteCommand(f"sudo rm -rf {download_utils.INSTALL_DIR}")
    vm.RemoteCommand("rm -rf /tmp/pgo_dir")
