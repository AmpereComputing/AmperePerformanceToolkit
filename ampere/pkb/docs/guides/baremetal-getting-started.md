# APT - Getting Started with Baremetal Systems

First, follow the steps to [install and setup APT](README.md)

## Run APT on a single node static system

Update the example `config.yml` below with the values specific to your SUT

```yaml
static_vms:
  - &vm1
    ip_address: <external_ip>
    user_name: <user>
    ssh_private_key: /path/to/ssh_key
    os_type: <os_type> # valid os_types can be found in perfkitbenchmark/os_types.py

ampere_vbench:
  vm_groups:
    default:
      static_vms:
        - *vm1
```

See possible values for `os_type` in `perfkitbenchmarker/os_types.py` and `ampere/pkb/os_types.py`

Either hard code the system information in the command, overwrite the YAML, or set environment variables e.g.

```bash
./pkb.py --benchmarks=ampere_vbench --benchmark_config_file=config.yml
```

## Run APT with a Client/Server Workload

When running a baremetal test there are a couple of things to keep in mind

- External IPs are used by APT in order to run remote SSH commands from the runner to the SUT
- Internal IPs are used between the Client and the SUT to make use of high-speed networking during a workload run

Example `config.yml`

```yaml
static_vms:
  - &server 
    ip_address: <external_ip>
    internal_ip: <internal_ip>
    user_name: <user> 
    ssh_private_key: /path/to/ssh_key
    os_type: <os_type> 
  - &client 
    ip_address: <external_ip>
    internal_ip: <internal_ip>
    user_name: <user> 
    ssh_private_key: /path/to/ssh_key
    os_type: <os_type> 
  
ampere_nginx_wrk:
  vm_groups:
    servers:
      static_vms:
        - *server
    clients:
      static_vms:
        - *client
```

