# Modifications Copyright (c) 2024 Ampere Computing LLC
# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing sysbench installation and cleanup functions."""


import logging
import posixpath
import time
from typing import List
from absl import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import flag_util
from ampere.pkb.common import download_utils
from ampere.pkb.linux_packages import mysql80

PACKAGE_NAME = "ampere_sysbench"
BENCHMARK_NAME = "ampere_mysql_sysbench"
FLAGS = flags.FLAGS

DISABLE = "disable"
UNIFORM = "uniform"
JMALLOC_VERSION = "5.1.0"

_IGNORE_CONCURRENT = flags.DEFINE_bool(
    f"{PACKAGE_NAME}_ignore_concurrent_modification",
    False,
    "If true, ignores concurrent modification P0001 exceptions thrown by "
    "some databases.",
)

flags.DEFINE_string(
    f"{PACKAGE_NAME}_db_engine", "mysql", "sysbench works on both mysql and pgsql"
)

flags.DEFINE_string(
    f"{PACKAGE_NAME}_git_branch",
    "4228c85ff9164aac93eb7af5ed8a622b039e3c5b",
    "sysbench git branch",
)


TABLE_COUNT = flags.DEFINE_integer(
    f"{PACKAGE_NAME}_table_count", 8, "sysbench table count"
)

TABLE_SIZE = flags.DEFINE_integer(
    f"{PACKAGE_NAME}_table_size", 10000000, "sysbench table size"
)

TABLE_COMPRESSION = flags.DEFINE_string(
    f"{PACKAGE_NAME}_table_compression", "of", "sysbench table compression"
)

WARM_TIME = flags.DEFINE_integer(f"{PACKAGE_NAME}_warm_time", 60, "sysbench warm time")

RUN_TIME = flags.DEFINE_integer(f"{PACKAGE_NAME}_run_time", 600, "sysbench run time")

flag_util.DEFINE_integerlist(
    f"{PACKAGE_NAME}_threads",
    [64],
    "Comma separated list of number of threads.Defaults to [1].",
)

flags.DEFINE_integer(
    f"{PACKAGE_NAME}_thread_init_timeout", 5, "sysbench thread timeout"
)

flags.DEFINE_integer(f"{PACKAGE_NAME}_rate", 0, "sysbench rate")

flags.DEFINE_string(f"{PACKAGE_NAME}_rand_type", "uniform", "sysbench random type")

flags.DEFINE_integer(f"{PACKAGE_NAME}_rand_seed", 1, "sysbench random seed")

SYSBENCH_DATA = flags.DEFINE_string(
    f"{PACKAGE_NAME}_data_lua",
    None,
    "Additional lua files for sysbench. "
    "Must be located in ./ampere/pkb/data/mysql/lua ",
)


flags.DEFINE_list(
    f"{PACKAGE_NAME}_workloads",
    "[oltp_point_select]",
    "comma separated list of workloads Defaults to oltp_point_select.",
)
# Different options for workloads: oltp_point_select,oltp_read_only,oltp_write_only,oltp_read_write

MYSQL_PORT = FLAGS[f"{mysql80.PACKAGE_NAME}_port"].value

MYSQL_PASSWORD = "123456"
PERCENT = "--percentile=95"

GIT_REPO = "https://github.com/akopytov/sysbench"
# release 1.0.20; committed Apr 24, 2020. When updating this, also update the
# correct line for CONCURRENT_MODS, as it may have changed in between releases.
SYSBENCH_DIR = posixpath.join(download_utils.INSTALL_DIR, "sysbench")
JMALLOC_DIR = posixpath.join(download_utils.INSTALL_DIR, "jmalloc")
# Inserts this error code on line 534.
CONCURRENT_MODS = (
    '534 i !strcmp(con->sql_state, "P0001")/* concurrent ' "modification */ ||"
)


def _Install(vm):
    """Installs Jmalloc on the VM."""
    # install openssl
    if (
        FLAGS["ampere_openssl_use"].value
        and not FLAGS[f"{BENCHMARK_NAME}_localhost"].value
    ):
        user_check, _ = vm.RemoteCommand("whoami")
        user_check = user_check.strip()
        vm.Install("ampere_openssl")
        vm.RemoteCommand(f"sed -i '/export/d' /home/{user_check}/.bashrc")
        vm.RemoteCommand(
            f" echo 'export PATH="
            f"{download_utils.INSTALL_DIR}/openssl/bin:$PATH' >> /home/{user_check}/.bashrc"
        )
        vm.RemoteCommand(f"source /home/{user_check}/.bashrc")
    vm.RemoteCommand(
        f"git clone https://github.com/jemalloc/jemalloc.git {JMALLOC_DIR}"
    )
    vm.RemoteCommand(f"cd {JMALLOC_DIR} && git checkout {JMALLOC_VERSION}")
    vm.RemoteCommand(f"cd {JMALLOC_DIR} && ./autogen.sh --with-lg-page=16")
    doc_jemalloc_path = posixpath.join(f"{JMALLOC_DIR}", "doc")
    vm.RemoteCommand(f"cd {doc_jemalloc_path} && touch jemalloc.html")
    vm.RemoteCommand(f"cd {doc_jemalloc_path} && touch jemalloc.3")
    vm.RemoteCommand(f"cd {JMALLOC_DIR} && ./configure")
    vm.RemoteCommand(f"cd {JMALLOC_DIR} && make -j")
    vm.RemoteCommand(f"cd {JMALLOC_DIR} && sudo make install")
    time.sleep(10)
    vm.RemoteCommand(f"git clone {GIT_REPO} {SYSBENCH_DIR}")
    git_branch = FLAGS[f"{PACKAGE_NAME}_git_branch"].value
    vm.RemoteCommand(f"cd {SYSBENCH_DIR} && git reset --hard {git_branch}")
    if _IGNORE_CONCURRENT.value:
        driver_file = f"{SYSBENCH_DIR}/src/drivers/pgsql/drv_pgsql.c"
        vm.RemoteCommand(f"sed -i '{CONCURRENT_MODS}' {driver_file}")
    vm.RemoteCommand(f"cd {SYSBENCH_DIR} && ./autogen.sh ")
    vm.RemoteCommand(
        f"cd {SYSBENCH_DIR} && export LD_LIBRARY_PATH=/opt/pkb/mysql/lib:$LD_LIBRARY_PATH && "
        f"./configure --with-mysql --without-pgsql --prefix {SYSBENCH_DIR} "
        f"--with-mysql-includes=/opt/pkb/mysql/include "
        f"--with-mysql-libs=/opt/pkb/mysql/lib"
    )
    vm.RemoteCommand(f"cd {SYSBENCH_DIR} && make -j`nproc` && sudo make install")


def YumInstall(vm):
    """Installs the sysbench package on the VM."""
    vm.InstallPackages(
        "make automake libtool pkgconfig libaio-devel git curl wget "
        "postgresql-devel libzstd-devel  zlib-devel perl"
    )
    if not FLAGS["ampere_openssl_use"].value:
        vm.InstallPackages("openssl-devel")
    _Install(vm)


def AptInstall(vm):
    """Installs the sysbench package on the VM."""
    vm.InstallPackages(
        "make automake libtool pkg-config libaio-dev default-libmysqlclient-dev "
        "libssl-dev libpq-dev"
    )
    _Install(vm)


def Configure(vm, mysql_vms):
    """Configure Mysql on 'vm'.

    Args:
      vm: VirtualMachine. The VM to configure.
      seed_vms: List of VirtualMachine. The seed virtual machine(s).
      no_of_instances: number of Mysql Instances
    """
    libtirpc_install_dir = posixpath.join(download_utils.INSTALL_DIR, "libtirpc")
    data_node_ips1 = mysql_vms.internal_ip
    for instance in range(FLAGS["ampere_mysql_instances"].value):
        port = MYSQL_PORT + instance
        table_compression = FLAGS[f"{PACKAGE_NAME}_table_compression"].value
        user_check, _ = vm.RemoteCommand("whoami")
        user_check = user_check.strip()
        check_openssl = ""
        if FLAGS["ampere_openssl_use"].value:
            check_openssl = (
                "export LD_LIBRARY_PATH=/opt/pkb/openssl/lib:/opt/pkb/openssl/lib64:"
                f"/opt/pkb/mysql/lib:{libtirpc_install_dir}/lib:$LD_LIBRARY_PATH; "
            )
        else:
            check_openssl = (
                f"export LD_LIBRARY_PATH=/opt/pkb/mysql/lib:"
                f"{libtirpc_install_dir}/lib:$LD_LIBRARY_PATH; "
            )
        query = (
            f"{check_openssl} LD_PRELOAD=/opt/pkb/jmalloc/lib/libjemalloc.so "
            f"{SYSBENCH_DIR}/bin/sysbench "
            f"{SYSBENCH_DIR}/share/sysbench/oltp_read_write.lua"
        )
        command_options = []
        command_options += [
            f" --table-size={TABLE_SIZE.value} ",
            f"--tables={TABLE_COUNT.value} ",
            f"--threads={TABLE_COUNT.value} ",
            "--mysql-user=sbtest ",
            f"--mysql-password={MYSQL_PASSWORD} ",
            f"--mysql-host={data_node_ips1}  ",
            f"--mysql-port={port} ",
        ]
        openssl_value = " --mysql-ssl=REQUIRED --mysql-ssl-cipher=AES128-SHA256 "
        compression_value = (
            ' --create_table_options="ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8"  '
        )
        prepare_command = "prepare >> ./data_prepare.log 2>&1"
        if FLAGS["ampere_openssl_use"].value:
            query = query + openssl_value
        if table_compression == "on":
            query = query + compression_value
        query = query + " ".join(command_options) + prepare_command
        vm.RemoteCommand(query)
        time.sleep(10)


def _GetSysbenchConnectionParameter(host):
    """Get Sysbench connection parameter."""
    engine_type = FLAGS[f"{PACKAGE_NAME}_db_engine"].value
    connection_string = []
    if engine_type == "mysql":
        connection_string += [
            f"--mysql-host={host}",
            f"--mysql-user={'sbtest'}",
            f"--mysql-password={MYSQL_PASSWORD}",
        ]
        if FLAGS["ampere_openssl_use"].value:
            connection_string += [
                "--mysql-ssl=REQUIRED --mysql-ssl-cipher=AES128-SHA256",
            ]
    elif engine_type == "pgsql":
        connection_string += [
            f"--pgsql-host={host}",
            f"--pgsql-user={'sbtest'}",
            f"--pgsql-password={MYSQL_PASSWORD}",
        ]
    return connection_string


def _GetCommonSysbenchOptions():
    """Get Sysbench options."""
    engine_type = FLAGS[f"{PACKAGE_NAME}_db_engine"].value
    result = []

    # Ignore possible mysql errors
    # https://github.com/actiontech/dble/issues/458
    # https://callisto.digital/posts/tools/using-sysbench-to-benchmark-mysql-5-7/
    if engine_type == "mysql":
        result += [
            f"--db-ps-mode={DISABLE}",
            # Error 1205: Lock wait timeout exceeded
            # Could happen when we overload the database
            "--mysql-ignore-errors=1213,1205,1020,2013",
            "--db-driver=mysql",
        ]
    elif engine_type == "pgsql":
        result += [
            "--db-driver=pgsql",
        ]
    return result


def _GetSysbenchCommand(
    duration, sysbench_thread, port, filename, mysql_host, vm, workload
):
    """Returns the sysbench command as a string."""
    if duration <= 0:
        raise ValueError("Duration must be greater than zero.")
    query = ""
    lua_template = SYSBENCH_DATA.value
    path = f"{SYSBENCH_DIR}/share/sysbench/{workload}.lua"
    check_file, _ = vm.RemoteCommand(
        f'if [ -f {path} ]; then echo "File found!" > checkfile.log;'
        f'  else echo "File not found!" > checkfile.log;  fi;'
        f"  cat checkfile.log;"
    )
    check_file = check_file.strip()
    if check_file == "File found!":
        logging.info(f"{workload}.lua is already present")
    else:
        vm.RemoteCopy(
            posixpath.join(lua_template, workload + ".lua"), f"/tmp/{workload}.lua"
        )
        vm.RemoteCommand(f"sudo chmod 777 /tmp/{workload}.lua")
        vm.RemoteCommand(
            f"sudo mv /tmp/{workload}.lua {SYSBENCH_DIR}/share/sysbench/{workload}.lua"
        )
    user_check, _ = vm.RemoteCommand("whoami")
    user_check = user_check.strip()
    libtirpc_install_dir = posixpath.join(download_utils.INSTALL_DIR, "libtirpc")
    check_openssl = ""
    if FLAGS["ampere_openssl_use"].value:
        check_openssl = (
            "export LD_LIBRARY_PATH=/opt/pkb/openssl/lib:/opt/pkb/openssl/lib64:"
            f"/opt/pkb/mysql/lib:{libtirpc_install_dir}/lib:$LD_LIBRARY_PATH; "
        )
    else:
        check_openssl = (
            f"export LD_LIBRARY_PATH=/opt/pkb/mysql/lib:"
            f"{libtirpc_install_dir}/lib:$LD_LIBRARY_PATH; "
        )
    cmd = [
        f"{check_openssl}  LD_PRELOAD=/opt/pkb/jmalloc/lib/libjemalloc.so "
        f"{SYSBENCH_DIR}/bin/sysbench "
        f"{SYSBENCH_DIR}/share/sysbench/{workload}.lua",
        f"--table-size={TABLE_SIZE.value:d}",
        f"--tables={TABLE_COUNT.value:d}",
        f"--mysql-port={port}",
        f"--mysql-db={'sbtest'}",
        f"--threads={sysbench_thread:d}",
        f"--events={0:d}",
        f"--time={duration:d}",
        f"--report-interval={10:d}",
        f"--thread-init-timeout={FLAGS[f'{PACKAGE_NAME}_thread_init_timeout'].value:d}",
        f"--rate={FLAGS[f'{PACKAGE_NAME}_rate'].value:d}",
        f"--rand-type={FLAGS[f'{PACKAGE_NAME}_rand_type'].value}",
        f"--rand-seed={FLAGS[f'{PACKAGE_NAME}_rand_seed'].value:d}",
    ]
    query = query + " ".join(
        cmd + _GetSysbenchConnectionParameter(mysql_host) + _GetCommonSysbenchOptions()
    )
    query = query + " run "
    time.sleep(10)
    vm.RemoteCommand("rm -rf checkfile.log")
    return query


# def _GetWarmSysbenchCommand(duration, sysbench_thread, port, mysql_host, vm):
#    """Returns the sysbench command as a string."""
#    if duration <= 0:
#        raise ValueError("Duration must be greater than zero.")
#    user_check, _ = vm.RemoteCommand("whoami")
#    user_check = user_check.strip()
#    libtirpc_install_dir = posixpath.join(download_utils.INSTALL_DIR, "libtirpc")
#    check_openssl = ""
#    if FLAGS["ampere_openssl_use"].value:
#        check_openssl = (
#            "export LD_LIBRARY_PATH=/opt/pkb/openssl/lib64:"
#            f"/opt/pkb/mysql/lib:{libtirpc_install_dir}/lib:$LD_LIBRARY_PATH; "
#        )
#    else:
#        check_openssl = (
#            f"export LD_LIBRARY_PATH=/opt/pkb/mysql/lib:"
#            f"{libtirpc_install_dir}/lib:$LD_LIBRARY_PATH; "
#        )
#    query = ""
#    cmd = [
#        f"{check_openssl}  LD_PRELOAD=/opt/pkb/jmalloc/lib/libjemalloc.so "
#        f"{SYSBENCH_DIR}/bin/sysbench "
#        f"{SYSBENCH_DIR}/share/sysbench/oltp_read_write.lua",
#        f"--table-size={TABLE_SIZE.value:d}",
#        f"--tables={TABLE_COUNT.value:d}",
#        f"--mysql-port={port}",
#        f"--mysql-db={'sbtest'}",
#        f"--threads={sysbench_thread:d}",
#    ]
#    query = query + " ".join(
#        cmd + _GetSysbenchConnectionParameter(mysql_host) + _GetCommonSysbenchOptions()
#    )
#    query = query + " prewarm "
#    return query


# def RunWarmupSysbenchOverAllPorts(mysql_vms, client, client_number, thread_num):
#    """
#    Starts warmup over all clients
#    Args:
#        mysql_vms:n
#        client: Client Object
#        client_number: Client Number
#
#    Returns:
#
#    """
#    total_instances = FLAGS["ampere_mysql_instances"].value
#    data_node_ips1 = mysql_vms.internal_ip
# warm up function
#    all_mysql_port = ""
#    for instance in range(total_instances):
#        port = MYSQL_PORT + instance
#        all_mysql_port = str(port) + "," + all_mysql_port
#    all_mysql_port = all_mysql_port[:-1]
#    run_cmd = _GetWarmSysbenchCommand(
#        WARM_TIME.value, thread_num, all_mysql_port, data_node_ips1, client
#    )
#    client.RobustRemoteCommand(run_cmd)
#    time.sleep(10)


def RunSysbenchOverAllPorts(mysql_vms, client, client_number, total_clients):
    """
    Runs sysbench over all the ports
    Args:
        mysql_vms:
        client: Client object
        client_number: client number

    Returns:

    """
    total_instances = FLAGS["ampere_mysql_instances"].value
    data_node_ips1 = mysql_vms.internal_ip
    thread_data = FLAGS[f"{PACKAGE_NAME}_threads"].value
    all_mysql_port = ""
    for instance in range(total_instances):
        port = MYSQL_PORT + instance
        all_mysql_port = str(port) + "," + all_mysql_port
    all_mysql_port = all_mysql_port[:-1]
    raw_result = []
    workload_data = FLAGS[f"{PACKAGE_NAME}_workloads"].value
    # Run warmup function
    # RunWarmupSysbenchOverAllPorts(mysql_vms, client, client_number, 8)
    for thread_num in thread_data:
        for workload in workload_data:
            filename = _ResultFilePath(client, workload, instance, thread_num)
            client.RemoteCommand(f"rm -rf {filename}")
            run_cmd = _GetSysbenchCommand(
                RUN_TIME.value,
                thread_num,
                all_mysql_port,
                filename,
                data_node_ips1,
                client,
                workload,
            )
            stdout, _ = client.RobustRemoteCommand(run_cmd, timeout=RUN_TIME.value + 60)
            metadata = GenerateMetadataFromFlags(total_clients, thread_num)
            raw_result_data = _ParseSysbenchLatency(
                thread_num, workload, stdout, metadata
            ) + _ParseSysbenchTransactions(workload, stdout, metadata)
            raw_result.append(raw_result_data)
    return raw_result


def _ParseSysbenchTransactions(
    workload, sysbench_output, metadata
) -> List[sample.Sample]:
    """Parse sysbench transaction results."""
    transactions_per_second = regex_util.ExtractFloat(
        r"transactions: *[0-9]* *\(([0-9]*[.]?[0-9]+) per sec.\)", sysbench_output
    )
    queries_per_second = regex_util.ExtractFloat(
        r"queries: *[0-9]* *\(([0-9]*[.]?[0-9]+) per sec.\)", sysbench_output
    )
    return [
        sample.Sample(f"{workload}_TPS", transactions_per_second, "tps", metadata),
        sample.Sample(f"{workload}_QPS", queries_per_second, "qps", metadata),
    ]


def _ParseSysbenchLatency(
    thread, workload, sysbench_output, metadata
) -> List[sample.Sample]:
    """Parse sysbench latency results."""
    percentile_latency = regex_util.ExtractFloat(
        "95th percentile: *([0-9]*[.]?[0-9]+)", sysbench_output.strip()
    )
    return [
        sample.Sample(f"{workload}_Threads", thread, "", metadata),
        sample.Sample(
            f"{workload}_95_Percentile_Latency", percentile_latency, "ms", metadata
        ),
    ]


def _ResultFilePath(vm, workload, instance, thread_num):
    """
    Generate Result File Path
    Args:
        vm:
        workload: Sysbench workload name
        instance: Server instance name
        thread_num: Client Threads

    Returns: sysbench result file path
    """
    sysbench_result = (
        f"{vm.hostname}.mysql_{workload}_thread{thread_num}_instance{instance}.log"
    )
    return posixpath.join(vm_util.VM_TMP_DIR, sysbench_result)


def GenerateMetadataFromFlags(total_clients, thread_num):
    """
    Generate MetadataFromFlags
    """
    metadata = {}

    metadata.update(
        {
            "num_server_nodes": 1,
            "num_client_nodes": total_clients,
            "num_instances": FLAGS["ampere_mysql_instances"].value,
            "mysql_database": "sbtest",
            "sysbench_thread_value": thread_num,
            "sysbench_run_time": RUN_TIME.value,
            "sysbench_tables": TABLE_COUNT.value,
            "sysbench_table_size": TABLE_SIZE.value,
        }
    )
    return metadata


def CleanNode(vm):
    """Remove Sysbench data from 'vm'.

    Args:
      vm: VirtualMachine. VM to clean.
    """
    if FLAGS["ampere_openssl_use"].value:
        user_check, _ = vm.RemoteCommand("whoami")
        user_check = user_check.strip()
        get_path, _ = vm.RemoteCommand(
            'echo `echo $PATH | tr ":" "\n" | grep -v "openssl" '
            '| tr "\n" ":"` > path.log && cat path.log'
        )
        get_path = get_path.strip()
        vm.RemoteCommand(f"sed -i '/export/d' /home/{user_check}/.bashrc")
        vm.RemoteCommand(f"echo 'export PATH={get_path}' >> /home/{user_check}/.bashrc")
        vm.RemoteCommand(f"source /home/{user_check}/.bashrc")
    vm.RemoteCommand(f"sudo rm -rf {download_utils.INSTALL_DIR}")
