# How To: Run APT with OCI

## Prerequisites:
 - Access to OCI
 - APT is setup on local machine

## Setting up Environment

1. Install `OCI CLI: [Quickstart Docs](https://docs.oracle.com/en-us/iaas/Content/API/SDKDocs/cliinstall.htm)
    - #### Mac OS:
        - `brew update && brew install oci-cli`
    - #### Linux/Unix:
        - `bash -c "$(curl -L https://raw.githubusercontent.com/oracle/oci-cli/master/scripts/install/install.sh)"`
    - Run `oci --version` to ensure cli is installed.

2. Run `oci setup config` to generate configuration file. 
    - This will generate API public and private keys. 
    - #### NOTE: From Oracle Console on Top Right Portion
        - User’s OCID: Found in users profile tab under OCID 
        - Tenancy ID: Found under tenancy tab under OCID
        - Regions and Availability Domains. This will typically be in your default Region / Domain for OCI User Account.
        - Keys need to be [added manually to OCI Console](https://docs.oracle.com/en-us/iaas/Content/API/Concepts/apisigningkey.htm#three)

3. Authenticate OCI Session
    - Run `oci session authenticate`
    - This needs to be done on local machine. 
        - Running this in a provisioned VM will not resolve in browser.

4. Configure `oci_cli_rc` file.
    - In addition to having a `.oci/config` file, a `.oci/oci_cli_rc` file must also be configured.
        - run `oci setup oci-cli-rc --file `<path/to/oci_cli_rc>`
        - copy credentials in `.oci/config` to `oci_cli_rc` file.
        - add in compartment-id as one of `oci_cli_rc` fields
            - [Find Compartment-ID in OCI](https://docs.oracle.com/en-us/iaas/Content/GSG/Tasks/contactingsupport_topic-Finding_the_OCID_of_a_Compartment.htm)
    - to run in different region, update the `region = <region-name>` in `oci_cli_rc` file.

System should now be configured to use APT to provision/cleanup OCI resources.

*Important Note*: be sure that both files `~/.oci/config` and `~/.oci/oci_cli_rc` are...

- Identical to reduce unexpected behavior, see this [issue](https://github.com/oracle/oci-cli/issues/674) for reference 
- Contain a block where the profile name is the same as the region that the yaml configuration file makes use of

e.g. if a benchmark yaml config uses `us-ashburn-1` as the region, both files should look like the following

```bash
[DEFAULT]
user=<user_id>
fingerprint=<fingerprint>
key_file=/path/to/key
tenancy=<tenancy_id>
region=us-ashburn-1
compartment-id=<compartment_id>

[us-ashburn-1]
user=<user_id>
fingerprint=<fingerprint>
key_file=/path/to/key
tenancy=<tenancy_id>
region=us-ashburn-1
compartment-id=<compartment_id>
```

## Running APT with OCI

See the example yaml config [here](ampere/pkb/configs/example_nginx.yml)

Once YAML is configured, run APT like any other workload.

`./pkb.py --benchmarks=<benchmark_name> --benchmark_config_file=/path/to/config $BENCHMARK_OPTIONS`
