import os
import uuid
from time import sleep, time
from datetime import datetime, timedelta

import pytest
import requests
from paramiko import SSHClient, AutoAddPolicy

DEFAULT_STORAGE_CLS = "harvester-longhorn"

DEFAULT_USER = "ubuntu"
DEFAULT_PASSWORD = "root"

DEFAULT_NODE_USERNAME = "rancher"
DEFAULT_NODE_PASSWORD = "root"

DOWNLOAD_ISO_DEFAULT_URL_FMT = "https://releases.rancher.com"
DOWNLOAD_ISO_PATH_FMT = "{URL}/harvester/{VERSION}/harvester-{VERSION}-amd64.iso"
DOWNLOAD_ISO_CHECKSUM_PATH_FMT = "{URL}/harvester/{VERSION}/harvester-{VERSION}-amd64.sha512"

NETWORK_VLAN_ID_LABEL = "network.harvesterhci.io/vlan-id"
UPGRADE_STATE_LABEL = "harvesterhci.io/upgradeState"
CONTROL_PLANE_LABEL = "node-role.kubernetes.io/control-plane"
NODE_INTERNAL_IP_ANNOTATION = "rke2.io/internal-ip"

LOGGING_NAMESPACE = "cattle-logging-system"
HARVESTER_NAMESPACE = "harvester-system"
LONGHORN_NAMESPACE = "longhorn-system"

expected_harvester_crds = {
    "addons.harvesterhci.io": False,
    "blockdevices.harvesterhci.io": False,
    "keypairs.harvesterhci.io": False,
    "preferences.harvesterhci.io": False,
    "settings.harvesterhci.io": False,
    "supportbundles.harvesterhci.io": False,
    "upgrades.harvesterhci.io": False,
    "versions.harvesterhci.io": False,
    "virtualmachinebackups.harvesterhci.io": False,
    "virtualmachineimages.harvesterhci.io": False,
    "virtualmachinerestores.harvesterhci.io": False,
    "virtualmachinetemplates.harvesterhci.io": False,
    "virtualmachinetemplateversions.harvesterhci.io": False,

    "clusternetworks.network.harvesterhci.io": False,
    "linkmonitors.network.harvesterhci.io": False,
    "nodenetworks.network.harvesterhci.io": False,
    "vlanconfigs.network.harvesterhci.io": False,
    "vlanstatuses.network.harvesterhci.io": False,

    "ksmtuneds.node.harvesterhci.io": False,

    "loadbalancers.loadbalancer.harvesterhci.io": False,
}

pytest_plugins = [
    "harvester_e2e_tests.fixtures.api_client",
]


def _lookup_image(api_client, name):
    code, data = api_client.images.get()
    if code == 200:
        for image in data['items']:
            if image['metadata']['name'] == name:
                return image
    return None


def _create_image(api_client, url, name, wait_timeout=300):
    image_data = _lookup_image(api_client, name)
    if image_data is None:
        code, image_data = api_client.images.create_by_url(name, url)
        assert code == 201, "failed to create image"

        endtime = datetime.now() + timedelta(seconds=wait_timeout)
        while endtime > datetime.now():
            code, data = api_client.images.get(name)
            if 100 == data.get('status', {}).get('progress', 0):
                break
            sleep(3)
        else:
            raise AssertionError(
                "Failed to create Image with error:\n"
                f"Status({code}): {data}"
            )

    return image_data


def _get_ip_from_vmi(vmi):
    assert _check_vm_ip_assigned(vmi), "virtual machine does not have ip assigned"
    return vmi['status']['interfaces'][0]['ipAddress']


def _check_vm_is_running(vmi):
    return ('status' in vmi and
            'phase' in vmi['status'] and
            'Running' == vmi['status']['phase'] and
            'nodeName' in vmi['status'] and
            vmi['status']['nodeName'] != "")


def _check_vm_ip_assigned(vmi):
    return ('status' in vmi and
            'interfaces' in vmi['status'] and
            len(vmi['status']['interfaces']) > 0 and
            'ipAddress' in vmi['status']['interfaces'][0] and
            vmi['status']['interfaces'][0]['ipAddress'] is not None)


def _check_assigned_ip_func(api_client, vm_name):
    def _check_assigned_ip():
        code, data = api_client.vms.get_status(vm_name)
        if code != 200:
            return False
        return _check_vm_is_running(data) and _check_vm_ip_assigned(data)
    return _check_assigned_ip


def _wait_for_vm_ready(request, api_client, vm_name):
    timeout = request.config.getoption('--wait-timeout') or 300
    endtime = datetime.now() + timedelta(seconds=timeout)
    while endtime > datetime.now():
        if _check_assigned_ip_func(api_client, vm_name)():
            break
        sleep(5)
    else:
        raise AssertionError("Time out while waiting for vm to be created")


def _wait_for_write_data(ip, timeout):
    client = SSHClient()
    # automatically add host since we only care about connectivity
    client.set_missing_host_key_policy(AutoAddPolicy)

    def _wait_for_connect():
        try:
            client.connect(ip, username=DEFAULT_USER, password=DEFAULT_PASSWORD)
        except Exception as e:
            print('Unable to connect to %s: %s' % (ip, e))
            return False

        script = f"echo {uuid.uuid4().hex} > ~/generate_str; sync"
        stdin, stdout, stderr = client.exec_command(script, get_pty=True)
        errors = stderr.read().strip().decode('utf-8')
        client.close()
        if errors != "":
            print('Failed to execute %s on %s: %s' % (script, ip, errors))
            return False
        return True

    endtime = datetime.now() + timedelta(seconds=timeout)
    while endtime > datetime.now():
        if _wait_for_connect():
            break
        sleep(5)
    else:
        raise AssertionError("Timed out while waiting for write data to vm")
    return True


def _get_data_from_vm(ip, timeout):
    client = SSHClient()
    # automatically add host since we only care about connectivity
    client.set_missing_host_key_policy(AutoAddPolicy)

    output = ""

    def _wait_for_connect():
        nonlocal output
        try:
            client.connect(ip, username=DEFAULT_USER, password=DEFAULT_PASSWORD)
        except Exception as e:
            print('Unable to connect to %s: %s' % (ip, e))
            return False

        script = "cat ~/generate_str"
        stdin, output, stderr = client.exec_command(script)
        errors = stderr.read().strip().decode('utf-8')
        client.close()

        assert errors == "", (
            f"Failed to execute {script} on {ip}: {errors}" % (script, ip, errors))
        return True

    endtime = datetime.now() + timedelta(seconds=timeout)
    while endtime > datetime.now():
        if _wait_for_connect():
            break
        sleep(3)
    else:
        raise AssertionError("Timed out while waiting for SSH server to be ready")

    return output.read().strip().decode('utf-8')


def _ping(vm_ip, target_ip):
    vm1_client = SSHClient()
    vm1_client.set_missing_host_key_policy(AutoAddPolicy)

    try:
        vm1_client.connect(vm_ip, username=DEFAULT_USER, password=DEFAULT_PASSWORD)
    except Exception as e:
        assert False, (
            'Unable to connect to %s: %s' % (vm_ip, e))

    script = f"ping -c1 {target_ip} > /dev/null && echo success || echo fail"
    stdin, stdout, stderr = vm1_client.exec_command(script)
    errors = stderr.read().strip().decode('utf-8')
    assert errors == "", (f"Failed to execute '{script}' on {vm_ip}: {errors}")
    result = stdout.read().strip().decode('utf-8')
    vm1_client.close()

    if result == 'success':
        return True
    return False


def _create_basic_vm(request, api_client, cluster_state, vm_prefix, sc="",
                     timeout=300):
    vm_name = f'{vm_prefix}-{cluster_state.unique_id}'

    vmspec = api_client.vms.Spec(1, 1, mgmt_network=False)

    image_id = (f"{cluster_state.ubuntu_image['metadata']['namespace']}/"
                f"{cluster_state.ubuntu_image['metadata']['name']}")
    vmspec.add_image(cluster_state.ubuntu_image['metadata']['name'], image_id, size=5)
    vmspec.add_volume("new-sc-disk", 5, storage_cls=sc)

    network_id = (f"{cluster_state.network['metadata']['namespace']}/"
                  f"{cluster_state.network['metadata']['name']}")
    vmspec.add_network("vlan", network_id)
    vmspec.user_data = f"""#cloud-config
chpasswd:
  expire: false
package_update: true
packages:
- qemu-guest-agent
password: {DEFAULT_PASSWORD}
runcmd:
- - systemctl
  - enable
  - --now
  - qemu-guest-agent.service
ssh_pwauth: true
"""

    code, data = api_client.vms.create(vm_name, vmspec)
    assert code == 201, (
        "Failed to create vm1: %s" % (data))

    _wait_for_vm_ready(request, api_client, vm_name)

    code, data = api_client.vms.get_status(vm_name)
    if code != 200:
        return None

    _wait_for_write_data(_get_ip_from_vmi(data), timeout)

    return data


def _create_version(request, api_client, version="master"):
    if version == "":
        version = "master"

    url = request.config.getoption('--upgrade-iso-cache-url') or DOWNLOAD_ISO_DEFAULT_URL_FMT
    isoURL = DOWNLOAD_ISO_PATH_FMT.format(URL=url, VERSION=version)

    checksum = ""
    if version[0] == "v":
        checksum_url = DOWNLOAD_ISO_CHECKSUM_PATH_FMT.format(
            URL=DOWNLOAD_ISO_DEFAULT_URL_FMT, VERSION=version)

        req = requests.get(checksum_url)
        assert req.status_code == 200, (f"Failed to get checksum from {checksum_url}")

        checksum = req.text.splitlines()[0].split()[0]

    return api_client.versions.create(version, isoURL, checksum)


def _get_all_nodes(api_client):
    code, data = api_client.hosts.get()
    assert code == 200, (
       f"Failed to get nodes: {code}, {data}")
    return data['data']


def _get_master_and_worker_nodes(api_client):
    nodes = _get_all_nodes(api_client)
    master_nodes = []
    worker_nodes = []
    for node in nodes:
        if CONTROL_PLANE_LABEL in node['metadata']['labels'] and \
           node['metadata']['labels'][CONTROL_PLANE_LABEL] == "true":

            master_nodes.append(node)
        else:
            worker_nodes.append(node)
    return master_nodes, worker_nodes


def _is_target_version(api_client, target_version):
    code, data = api_client.upgrades.get()
    if code == 200 and len(data['items']) > 0:
        for upgrade in data['items']:
            if 'spec' in upgrade and \
               'version' in upgrade['spec'] and \
               upgrade['spec']['version'] == target_version and \
               'labels' in upgrade['metadata'] and \
               UPGRADE_STATE_LABEL in upgrade['metadata']['labels'] and \
               upgrade['metadata']['labels'][UPGRADE_STATE_LABEL] == 'Succeeded':
                return True
    return False


@pytest.fixture(scope="class")
def network(request, api_client, cluster_state):
    code, data = api_client.networks.get()
    assert code == 200, (
        "Failed to get networks: %s" % (data))

    vlan_id = request.config.getoption('--vlan-id') or 1
    for network in data['items']:
        if NETWORK_VLAN_ID_LABEL in network['metadata']['labels'] and \
           network['metadata']['labels'][NETWORK_VLAN_ID_LABEL] == f"{vlan_id}":
            cluster_state.network = network
            return

    raise AssertionError("Failed to find a routable vlan network")


@pytest.fixture(scope="class")
def unique_id(cluster_state, unique_name):
    cluster_state.unique_id = unique_name


@pytest.fixture(scope="class")
def cluster_state():
    class ClusterState:
        vm1 = None
        vm2 = None
        vm3 = None
        pass

    return ClusterState()


@pytest.fixture(scope="class")
def cluster_prereq(unique_id, network, base_sc, ubuntu_image, new_sc):
    pass


@pytest.fixture(scope='class')
def ubuntu_image(request, api_client, cluster_state):
    image_name = "focal-server-cloudimg-amd64"

    base_url = 'https://cloud-images.ubuntu.com/focal/current/'

    cache_url = request.config.getoption('--image-cache-url')
    if cache_url:
        base_url = cache_url
    url = os.path.join(base_url, 'focal-server-cloudimg-amd64.img')

    image_json = _create_image(api_client, url, name=image_name)
    cluster_state.ubuntu_image = image_json
    return image_json


@pytest.fixture(scope="class")
def openSUSE_image(request, api_client, cluster_state):
    image_name = "opensuse-leap-15-4"

    base_url = ('https://repo.opensuse.id//repositories/Cloud:/Images:'
                '/Leap_15.4/images')

    cache_url = request.config.getoption('--image-cache-url')
    if cache_url:
        base_url = cache_url
    url = os.path.join(base_url, 'openSUSE-Leap-15.4.x86_64-NoCloud.qcow2')

    image_json = _create_image(api_client, url, name=image_name)
    cluster_state.openSUSE_image = image_json
    return image_json


def _vm1_backup(api_client, cluster_state, timeout=300):
    backup_name = f"vm1-backup-{cluster_state.unique_id}"
    code, data = api_client.vms.backup(cluster_state.vm1['metadata']['name'], backup_name)
    assert code == 204, (
        f"Failed to backup vm: {data}")

    def _wait_for_backup():
        nonlocal data
        code, data = api_client.backups.get(backup_name)
        assert code == 200, (
            f"Failed to get backup {backup_name}: {data}")

        if "status" in data and \
           "readyToUse" in data["status"] and \
           data["status"]["readyToUse"] is True:
            return True
        return False

    endtime = datetime.now() + timedelta(seconds=timeout)
    while endtime > datetime.now():
        if _wait_for_backup():
            break
        sleep(5)
    else:
        raise AssertionError("Time out while waiting for backup to be created")

    return data


@pytest.fixture(scope="class")
def base_sc(request, api_client, cluster_state):
    code, data = api_client.scs.get()
    assert code == 200, (f"Failed to get storage classes: {data}")

    for sc in data['items']:
        if "base-sc" in sc['metadata']['name']:
            cluster_state.base_sc = sc
            return

    sc_name = f"base-sc-{cluster_state.unique_id}"
    cluster_state.base_sc = _create_default_storage_class(request, api_client, sc_name)


@pytest.fixture(scope="class")
def new_sc(request, api_client, cluster_state):
    code, data = api_client.scs.get()
    assert code == 200, (f"Failed to get storage classes: {data}")

    for sc in data['items']:
        if "new-sc" in sc['metadata']['name']:
            cluster_state.new_sc = sc
            return

    sc_name = f"new-sc-{cluster_state.unique_id}"
    cluster_state.new_sc = _create_default_storage_class(request, api_client, sc_name)


def _create_default_storage_class(request, api_client, name):
    replicas = request.config.getoption('--upgrade-sc-replicas') or 3

    code, data = api_client.scs.get(name)
    if code != 200:
        code, data = api_client.scs.create(name, replicas)
        assert code == 201, (
            f"Failed to create new storage class {name}: {data}")

    sc_data = data

    code, data = api_client.scs.set_default(name)
    assert code == 200, (
        f"Failed to set default storage class {name}: {data}")

    return sc_data


@pytest.fixture(scope="class")
def vm_prereq(cluster_state, request, api_client):
    # create new storage class, make it default
    # create 3 VMs:
    # - having the new storage class
    # - the VM that have some data written, take backup
    # - the VM restored from the backup

    timeout = request.config.getoption('--wait-timeout') or 300

    cluster_state.vm1 = _create_basic_vm(request, api_client, cluster_state, vm_prefix="vm1",
                                         timeout=timeout,
                                         sc=cluster_state.base_sc['metadata']['name'])
    cluster_state.backup = _vm1_backup(api_client, cluster_state, timeout)

    vm2_name = f"vm2-{cluster_state.unique_id}"
    restoreSpec = api_client.backups.RestoreSpec(True, vm_name=vm2_name)
    code, data = api_client.backups.create(cluster_state.backup['metadata']['name'], restoreSpec)

    assert code == 201, (
        f"Failed to restore to vm2: {data}")

    endtime = datetime.now() + timedelta(seconds=timeout)
    while endtime > datetime.now():
        if _check_assigned_ip_func(api_client, vm2_name)():
            break
        sleep(5)
    else:
        raise AssertionError("Time out while waiting for assigned ip for vm2")

    code, cluster_state.vm2 = api_client.vms.get_status(vm2_name)
    assert code == 200, (
        f"Failed to get vm2 vmi: {data}")

    # verify data
    vm1_data = _get_data_from_vm(_get_ip_from_vmi(cluster_state.vm1), timeout)
    vm2_data = _get_data_from_vm(_get_ip_from_vmi(cluster_state.vm2), timeout)

    assert vm1_data == vm2_data, ("Data in VM is lost")

    # check VMs should able to reach each others (in same networks)
    assert _ping(_get_ip_from_vmi(cluster_state.vm1), _get_ip_from_vmi(cluster_state.vm2)), (
        "Failed to ping each other")

    cluster_state.vm3 = _create_basic_vm(request, api_client, cluster_state, vm_prefix="vm3",
                                         timeout=timeout,
                                         sc=cluster_state.new_sc['metadata']['name'])


@pytest.mark.upgrade
@pytest.mark.negative
class TestInvalidUpgrade:
    VM_PREFIX = "vm-degraded-volume"

    def _create_vm(self, request, api_client, cluster_state):
        return _create_basic_vm(request, api_client, cluster_state, self.VM_PREFIX,
                                sc=DEFAULT_STORAGE_CLS)

    def _degrad_volume(self, api_client, pvc_name):
        code, data = api_client.volumes.get(name=pvc_name)
        assert code == 200, (
            f"Failed to get volume {pvc_name}: {data}")

        volume = data
        volume_name = volume["spec"]["volumeName"]

        code, data = api_client.lhreplicas.get()
        assert code == 200 and len(data), (
            f"Failed to get longhorn replicas or have no replicas: {data}")

        replicas = data["items"]
        for k in range(len(replicas)):
            replica = replicas[k]
            if replica["spec"]["volumeName"] == volume_name:
                api_client.lhreplicas.delete(name=replica["metadata"]["name"])
                break

        # wait for volume be degraded status
        sleep(10)

    def _upgrade(self, request, api_client, version):
        code, data = _create_version(request, api_client, version)
        assert code == 201, (
            f"Failed to create version {version}: {data}")

        code, data = api_client.upgrades.create(version)
        assert code == 400, (
            f"Failed to verify degraded volume: {code}, {data}")

        return data

    def _clean_degraded_volume(self, api_client, version):
        code, data = api_client.vms.delete(self.vm["metadata"]["name"])
        assert code == 200, (
            f"Failed to delete vm {self.vm['metadata']['name']}: {data}")

        code, data = api_client.versions.delete(version)
        assert code == 204, (
            f"Failed to delete version {version}: {data}")

    def test_degraded_volume(self, cluster_prereq, request, api_client, cluster_state):
        """
        Criteria: create upgrade should fails if there are any degraded volumes
        Steps:
        1. Create a VM using a volume with 3 replicas.
        2. Delete one replica of the volume. Let the volume stay in
           degraded state.
        3. Immediately upgrade Harvester.
        4. Upgrade should fail.
        """
        self.vm = self._create_vm(request, api_client, cluster_state)

        claimName = self.vm["spec"]["volumes"][0]["persistentVolumeClaim"]["claimName"]
        self._degrad_volume(api_client, claimName)

        version = uuid.uuid4().hex
        self._upgrade(request, api_client, version)

        self._clean_degraded_volume(api_client, version)

    # TODO: waiting for https://github.com/harvester/harvester/issues/3310 to be fixed
    @pytest.mark.skip()
    def test_invalid_manifest(self, api_client):
        """
        Criteria: https://github.com/harvester/tests/issues/518
        Steps:
        1. Create an invalid manifest.
        2. Try to upgrade with the invalid manifest.
        3. Upgrade should not start and fail.
        """
        # version_name = "v0.0.0"

        # code, data = api_client.versions.get(version_name)
        # if code != 200:
        #     code, data = api_client.versions.create(version_name, "https://invalid_version_url")
        #     assert code == 201, (
        #         "Failed to create invalid version: %s", data)

        # code, data = api_client.upgrades.create(version_name)
        # assert code == 201, (
        #     "Failed to create invalid upgrade: %s", data)


@pytest.mark.upgrade
@pytest.mark.any_nodes
class TestAnyNodesUpgrade:

    def _force_create_version(self, request, api_client, version):
        code, data = api_client.versions.get(version)
        if code == 200:
            code, data = api_client.versions.delete(version)
            assert code == 204, (
                f"Failed to delete version {version}: {data}")

        code, data = _create_version(request, api_client, version)
        assert code == 201, (
            f"Failed to create version {version}: {data}")

    @pytest.mark.dependency(name="any_nodes_upgrade")
    def test_perform_upgrade(self, cluster_prereq, vm_prereq, request, api_client):
        """
        - perform upgrade
        - check all nodes upgraded
        """
        timeout = request.config.getoption('--wait-timeout') or 7200
        version = request.config.getoption("--upgrade-target-version") or "master"
        if _is_target_version(api_client, version):
            return

        self._force_create_version(request, api_client, version)

        code, data = api_client.upgrades.create(version)
        assert code == 201, (
            f"Failed to upgrade version {version}: {code}, {data}")

        def _wait_for_upgrade():
            try:
                code, upgrade_data = api_client.upgrades.get(data["metadata"]["name"])
                if code != 200:
                    return False
            except Exception:
                sleep(60)
                return False

            if "labels" in upgrade_data["metadata"] and \
               UPGRADE_STATE_LABEL in upgrade_data["metadata"]["labels"] and \
               upgrade_data["metadata"]["labels"][UPGRADE_STATE_LABEL] == "Succeeded":
                return True

            if "status" in upgrade_data and "conditions" in upgrade_data["status"]:
                conds = upgrade_data["status"]["conditions"]
                if len(conds) > 0:
                    for cond in conds:
                        if cond["status"] == "False":
                            cond_type = cond["type"]
                            raise AssertionError(f"Upgrade failed: {cond_type}: {cond}")

                        if cond["type"] == "Completed" and cond["status"] == "True":
                            return True

            return False

        upgrade_timeout = request.config.getoption('--upgrade-wait-timeout') or 7200
        endtime = datetime.now() + timedelta(seconds=upgrade_timeout)
        while endtime > datetime.now():
            if _wait_for_upgrade():
                break
            sleep(5)
        else:
            raise AssertionError("Upgrade timeout")

        # start vms when are stopped
        code, data = api_client.vms.get()
        assert code == 200, (f"Failed to get vms: {data}")

        for vm in data["data"]:
            if "ready" not in vm["status"] or not vm["status"]["ready"]:
                code, data = api_client.vms.start(vm["metadata"]["name"])
                assert code == 204, (f"Failed to start vm {vm['metadata']['name']}: {data}")

                # wait for vm to be assigned an IP
                endtime = datetime.now() + timedelta(seconds=timeout)
                while endtime > datetime.now():
                    if _wait_for_vm_ready(request, api_client, vm["metadata"]["name"]):
                        break
                    sleep(5)
                else:
                    raise AssertionError("start vm timeout")

    @pytest.mark.dependency(depends=["any_nodes_upgrade"])
    def test_verify_logging(self, request, api_client):
        """ Verify logging pods and logs
        Criteria: https://github.com/harvester/tests/issues/535
        """

        pods = api_client.get_pods(namespace=LOGGING_NAMESPACE)
        assert len(pods) > 0, "No logging pods found"

        for pod in pods:
            # Verify pod is running or completed
            assert pod["status"]["phase"] == "Running" or pod["status"]["phase"] == "Succeeded", (
                'Pod %s is not running or completed' % pod["metadata"]["name"])

        # verify kube-apiserver audit log of master node
        masters, workers = _get_master_and_worker_nodes(api_client)
        assert len(masters) > 0, "No master nodes found"

        username = "rancher"
        password = request.config.getoption('--host-password') or "root"

        timeout = request.config.getoption('--wait-timeout') or 300

        ticks = int(time() - 86400 * 2)
        script = ("last_received=$(sudo tail /var/lib/rancher/rke2/server/logs/audit.log"
                  "| awk 'END{print}' | jq .requestReceivedTimestamp "
                  "| xargs -I {} date -d \"{}\" +%s);"
                  "echo $last_received;"
                  f"echo $(if [ \"$last_received\" -gt \"{ticks}\" ]; then echo \"True\";fi)")

        client = SSHClient()
        client.set_missing_host_key_policy(AutoAddPolicy)
        for node in masters:
            node_ip = node["metadata"]["annotations"][NODE_INTERNAL_IP_ANNOTATION]
            output = ""

            def _wait_for_audit_log_updated():
                nonlocal output
                try:
                    client.connect(node_ip, username=username, password=password)
                except Exception as e:
                    print(f"Unable to connect to {node_ip}: {e}")
                    return False

                stdin, stdout, stderr = client.exec_command(script, get_pty=True)
                stdin.write(password + '\n')
                results = stdout.read().splitlines()
                errors = stderr.read().strip().decode('utf-8')
                client.close()
                if errors != "":
                    print(f"Failed to execute {script} on {node_ip}: {errors}")
                    return False

                output = results[len(results)-1].decode('utf-8')
                return True

            endtime = datetime.now() + timedelta(seconds=timeout)
            while endtime > datetime.now():
                if _wait_for_audit_log_updated():
                    break
                sleep(5)
            else:
                raise AssertionError("Timed out while waiting for SSH server to be ready")

            assert "True" == output, (
                f"Node {node_ip} audit log is not updated")

    @pytest.mark.dependency(depends=["any_nodes_upgrade"])
    def test_verify_network(self, request, api_client, cluster_state):
        """ Verify cluster and VLAN networks
        - cluster network `mgmt` should exists
        - Created VLAN should exists
        """

        code, cnets = api_client.clusternetworks.get()
        assert code == 200, (
            "Failed to get Networks: %d, %s" % (code, cnets))

        assert len(cnets["items"]) > 0, ("No Networks found")

        mgmt_exists = False

        for net in cnets["items"]:
            if net["metadata"]["name"] == "mgmt":
                mgmt_exists = True

        assert mgmt_exists, ("Cluster network mgmt not found")

        code, vnets = api_client.networks.get()
        assert code == 200, (f"Failed to get VLANs: {code}, {vnets}" % (code, vnets))
        assert len(vnets["items"]) > 0, ("No VLANs found")

        vlan_exists = False
        for net in vnets["items"]:
            if net["metadata"]["name"] == cluster_state.network['metadata']['name']:
                vlan_exists = True

        assert vlan_exists, ("VLAN not found")

    @pytest.mark.dependency(depends=["any_nodes_upgrade"])
    def test_verify_vms(self, request, api_client, cluster_state):
        """ Verify VMs' state and data
        Criteria:
        - VMs should keep in running state
        - data in VMs should not lost
        """

        timeout = request.config.getoption('--wait-timeout') or 300

        code, vmis = api_client.vms.get_status()
        assert code == 200, (
            f"Failed to get VMs: {code}, {vmis}")

        assert len(vmis["data"]) > 0, ("No VMs found")

        for vmi in vmis["data"]:
            assert _check_vm_is_running(vmi), (
                f"VM {vmi['metadata']['name']} is not running")

        vm1_data = _get_data_from_vm(_get_ip_from_vmi(cluster_state.vm1), timeout)
        vm2_data = _get_data_from_vm(_get_ip_from_vmi(cluster_state.vm2), timeout)
        assert vm1_data == vm2_data, ("Data in VM is lost")

    @pytest.mark.dependency(depends=["any_nodes_upgrade"])
    def test_verify_restore_vm(self, request, api_client, cluster_state):
        """ Verify VM restored from the backup
        Criteria:
        - VM should able to start
        - data in VM should not lost
        """

        timeout = request.config.getoption('--wait-timeout') or 300

        vm4_name = f"vm4-{cluster_state.unique_id}"
        restoreSpec = api_client.backups.RestoreSpec(True, vm_name=vm4_name)
        code, data = api_client.backups.create(cluster_state.backup['metadata']['name'],
                                               restoreSpec)
        assert code == 201, (
            f"Failed to restore to vm4: {data}")

        endtime = datetime.now() + timedelta(seconds=timeout)
        while endtime > datetime.now():
            if _check_assigned_ip_func(api_client, vm4_name)():
                break
            sleep(5)
        else:
            raise AssertionError("Time out while waiting for assigned ip for vm4")

        code, vm4 = api_client.vms.get_status(vm4_name)
        assert code == 200, (
            f"Failed to get vm2 vmi: {vm4}")

        vm4_data = _get_data_from_vm(_get_ip_from_vmi(vm4), timeout)
        vm1_data = _get_data_from_vm(_get_ip_from_vmi(cluster_state.vm1), timeout)
        assert vm1_data == vm4_data, ("Data in VM is not the same as the original")

    @pytest.mark.dependency(depends=["any_nodes_upgrade"])
    def test_verify_storage_class(self, api_client):
        """ Verify StorageClasses and defaults
        - `new_sc` should be settle as default
        - `longhorn` should exists
        """
        code, scs = api_client.scs.get()
        assert code == 200, (
            "Failed to get StorageClasses: %d, %s" % (code, scs))

        assert len(scs["items"]) > 0, ("No StorageClasses found")

        longhorn_exists = False
        test_exists = False
        test_default = False
        for sc in scs["items"]:
            annotations = sc["metadata"].get("annotations", {})
            if sc["metadata"]["name"] == "longhorn":
                longhorn_exists = True

            if "new-sc" in sc["metadata"]["name"]:
                test_exists = True
                default = annotations["storageclass.kubernetes.io/is-default-class"]
                if default == "true":
                    test_default = True

        assert longhorn_exists, ("longhorn StorageClass not found")
        assert test_exists, ("test StorageClass not found")
        assert test_default, ("test StorageClass is not default")

    @pytest.mark.dependency(depends=["any_nodes_upgrade"])
    def test_verify_os_version(self, request, api_client):
        # Verify /etc/os-release on all nodes

        client = SSHClient()
        client.set_missing_host_key_policy(AutoAddPolicy)

        timeout = request.config.getoption('--wait-timeout')
        script = "cat /etc/os-release"
        version = request.config.getoption("--upgrade-target-version") or "master"
        timeout = request.config.getoption('--wait-timeout') or 300

        # Get all nodes
        nodes = _get_all_nodes(api_client)
        for node in nodes:
            node_ip = node["metadata"]["annotations"][NODE_INTERNAL_IP_ANNOTATION]
            output = ""

            def _wait_for_check_os_release():
                nonlocal output
                try:
                    client.connect(node_ip, username=DEFAULT_NODE_USERNAME,
                                   password=DEFAULT_NODE_PASSWORD)
                except Exception as e:
                    print('Unable to connect to %s: %s' % (node_ip, e))
                    return False

                stdin, stdout, stderr = client.exec_command(script, get_pty=True)
                results = stdout.read().splitlines()
                errors = stderr.read().strip().decode('utf-8')
                client.close()
                if errors != "":
                    print('Failed to execute %s on %s: %s' % (script, node_ip, errors))
                    return False

                output = results[3].decode('utf-8')
                return True

            endtime = datetime.now() + timedelta(seconds=timeout)
            while endtime > datetime.now():
                if _wait_for_check_os_release():
                    break
                sleep(5)
            else:
                raise AssertionError("Timed out while waiting for SSH server to be ready")
            assert version in output, ("OS version is not correct")

    @pytest.mark.dependency(depends=["any_nodes_upgrade"])
    def test_verify_rke2_version(self, request, api_client):
        # Verify node version on all nodes

        client = SSHClient()
        client.set_missing_host_key_policy(AutoAddPolicy)
        timeout = request.config.getoption('--wait-timeout') or 300
        script = "cat /etc/harvester-release.yaml"

        # Verify rke2 version
        except_rke2_version = ""
        masters, workers = _get_master_and_worker_nodes(api_client)
        for node in masters:
            node_ip = node["metadata"]["annotations"][NODE_INTERNAL_IP_ANNOTATION]

            # Get except rke2 version
            if except_rke2_version == "":
                def _wait_for_check_os_release():
                    nonlocal except_rke2_version
                    try:
                        client.connect(node_ip, username=DEFAULT_NODE_USERNAME,
                                       password=DEFAULT_NODE_PASSWORD)
                    except Exception as e:
                        print('Unable to connect to %s: %s' % (node_ip, e))
                        return False

                    stdin, stdout, stderr = client.exec_command(script, get_pty=True)
                    results = stdout.read().splitlines()
                    errors = stderr.read().strip().decode('utf-8')
                    client.close()
                    if errors != "":
                        print('Failed to execute %s on %s: %s' % (script, node_ip, errors))
                        return False

                    for line in results:
                        if "kubernetes" in line.decode('utf-8'):
                            except_rke2_version = line.decode('utf-8').split(":")[1].strip()
                            break
                    return True

                endtime = datetime.now() + timedelta(seconds=timeout)
                while endtime > datetime.now():
                    if _wait_for_check_os_release():
                        break
                    sleep(5)
                else:
                    raise AssertionError("Timed out while waiting for SSH server to be ready")
                assert except_rke2_version != "", ("Failed to get except rke2 version")

            if "status" in node and "nodeInfo" in node["status"]:
                assert except_rke2_version == node["status"]["nodeInfo"]["kubeletVersion"], (
                    "rke2 version is not correct")

    @pytest.mark.dependency(depends=["any_nodes_upgrade"])
    def test_verify_deployed_components_version(self, api_client):
        """ Verify deployed kubevirt and longhorn version
        Criteria:
        - except version(get from apps.catalog.cattle.io/harvester) should be equal to the version
          of kubevirt and longhorn
        """

        kubevirt_version_existed = False
        engine_image_version_existed = False
        longhorn_manager_version_existed = False

        # Get except version from apps.catalog.cattle.io/harvester
        apps = api_client.get_apps_catalog(namespace=HARVESTER_NAMESPACE, name="harvester")
        assert apps['type'] != "error", (
            f"Failed to get apps.catalog.cattle.io/harvester: {apps['message']}")

        # Get except image of kubevirt and longhorn
        kubevirt_operator = (
            apps['spec']['chart']['values']['kubevirt-operator']['containers']['operator'])
        kubevirt_operator_image = (
            f"{kubevirt_operator['image']['repository']}:{kubevirt_operator['image']['tag']}")

        longhorn = apps['spec']['chart']['values']['longhorn']['image']['longhorn']
        longhorn_images = {
            "engine-image": f"{longhorn['engine']['repository']}:{longhorn['engine']['tag']}",
            "longhorn-manager": f"{longhorn['manager']['repository']}:{longhorn['manager']['tag']}"
        }

        # Verify kubevirt version
        pods = api_client.get_pods(namespace=HARVESTER_NAMESPACE)
        assert len(pods) > 0, f"Failed to get pods in namespace {HARVESTER_NAMESPACE}"

        for pod in pods:
            if "virt-operator" in pod['metadata']['name']:
                kubevirt_version_existed = (
                    kubevirt_operator_image == pod['spec']['containers'][0]['image'])

        # Verify longhorn version
        pods = api_client.get_pods(namespace=LONGHORN_NAMESPACE)
        assert len(pods) > 0, f"Failed to get pods in namespace {LONGHORN_NAMESPACE}"

        for pod in pods:
            if "longhorn-manager" in pod['metadata']['name']:
                longhorn_manager_version_existed = (
                  longhorn_images["longhorn-manager"] == pod['spec']['containers'][0]['image'])
            elif "engine-image" in pod['metadata']['name']:
                engine_image_version_existed = (
                    longhorn_images["engine-image"] == pod['spec']['containers'][0]['image'])

        assert kubevirt_version_existed, "kubevirt version is not correct"
        assert engine_image_version_existed, "longhorn engine image version is not correct"
        assert longhorn_manager_version_existed, "longhorn manager version is not correct"

    @pytest.mark.dependency(depends=["any_nodes_upgrade"])
    def test_verify_crds_existed(self, api_client):
        """ Verify crds existed
        Criteria:
        - crds should be existed
        """
        not_existed_crds = []
        exist_crds = True
        for crd in expected_harvester_crds:
            result = api_client.get_crds(name=crd)
            if result['type'] == "error":
                exist_crds = False
                not_existed_crds.append(crd)

        if not exist_crds:
            raise AssertionError(f"CRDs {not_existed_crds} are not existed")
