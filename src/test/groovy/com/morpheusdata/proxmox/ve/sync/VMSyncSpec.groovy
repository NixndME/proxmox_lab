package com.morpheusdata.proxmox.ve.sync

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.providers.CloudProvider
import com.morpheusdata.core.services.CloudService
import com.morpheusdata.core.services.ComputeServerService
import com.morpheusdata.core.services.ComputeServerTypeService
import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.model.*
import com.morpheusdata.model.projection.ComputeServerIdentityProjection
import com.morpheusdata.proxmox.ve.ProxmoxVePlugin
import com.morpheusdata.proxmox.ve.util.ProxmoxApiComputeUtil
import groovy.json.JsonOutput
import io.reactivex.Observable
import spock.lang.Specification
import spock.lang.Subject

class VMSyncSpec extends Specification {

    @Subject
    VMSync vmSync

    // Mocks
    ProxmoxVePlugin mockPlugin
    Cloud mockCloud
    HttpApiClient mockApiClient
    CloudProvider mockCloudProvider
    MorpheusContext mockMorpheusContext

    // Mock Morpheus Services
    ComputeServerService mockComputeServerService
    CloudService.Async mockCloudServiceAsync // For hostIdentitiesMap
    ComputeServerTypeService.Async mockComputeServerTypeServiceAsync


    void setup() {
        mockPlugin = Mock(ProxmoxVePlugin)
        mockCloud = Mock(Cloud)
        mockApiClient = Mock(HttpApiClient) // apiClient is passed to ProxmoxApiComputeUtil, not directly used by VMSync for HTTP
        mockCloudProvider = Mock(CloudProvider)
        mockMorpheusContext = Mock(MorpheusContext)

        // Mock services that VMSync might use via MorpheusContext
        mockComputeServerService = Mock(ComputeServerService.Async)
        mockMorpheusContext.async.computeServer >> mockComputeServerService

        // For hostIdentitiesMap in addMissingVirtualMachines
        // This requires mocking the chain of calls to get host identities
        // ComputeServerService.Async mockComputeServerServiceAsync = Mock()
        // mockMorpheusContext.async.computeServer >> mockComputeServerServiceAsync
        // This setup is simplified. A real test might need more detailed Observable/Flowable mocking for reactive calls.
        Observable<ComputeServerIdentityProjection> hostProjections = Observable.empty() // Default to no hosts
        mockComputeServerService.listIdentityProjections(mockCloud.id, null) >> hostProjections


        // Setup VMSync instance
        // Note: ProxmoxApiComputeUtil.listVMs is called externally to VMSync's execute,
        // so VMSync receives the `cloudItems` as a parameter to its internal methods.
        // We don't mock ProxmoxApiComputeUtil here directly for `listVMs` call in `execute`,
        // but we rely on testing its output separately.
        vmSync = new VMSync(mockPlugin, mockCloud, mockApiClient, mockCloudProvider)
        vmSync.context = mockMorpheusContext // Ensure the mock context is used

        // Default behaviors for mocks
        mockCloud.getAccount() >> new Account(id: 1L)
        mockCloud.getId() >> 100L // Example cloud ID for category
        // For computeServerType in addMissingVirtualMachines
        mockCloudProvider.getComputeServerTypes() >> [
            new ComputeServerType(code: 'proxmox-qemu-vm-unmanaged', name: 'Proxmox VM Unmanaged')
        ]
    }

    def "addMissingVirtualMachines correctly creates ComputeServer objects from cloudItem data"() {
        given:
        def cloudItem = [
            vmid            : "101",
            name            : "test-vm-01",
            node            : "proxmox-node-1",
            ip              : "192.168.1.101",
            status          : "running",
            maxmem          : 2147483648L, // 2GB
            maxCores        : 2,
            coresPerSocket  : 1,
            disk            : 5368709120L, // 5GB used disk (from PVE's 'maxdisk' which is used space)
            disks           : [
                [name: "virtio0", type: "virtio", storage: "local-lvm", file: "local-lvm:vm-101-disk-0", sizeRaw: "20G", sizeBytes: 20L * 1024L * 1024L * 1024L, format: "qcow2"],
                [name: "virtio1", type: "virtio", storage: "local-lvm", file: "local-lvm:vm-101-disk-1", sizeRaw: "5G", sizeBytes: 5L * 1024L * 1024L * 1024L, format: "raw"]
            ],
            networkInterfaces: [
                [name: "net0", model: "virtio", macAddress: "AA:BB:CC:00:11:22", bridge: "vmbr0", vlanTag: "10"]
            ],
            qemuAgent       : [
                status: "running",
                data  : [id: "ubuntu", name: "Ubuntu", version: "20.04", ostype: "linux"],
                networkInterfaces: [ [name: "ens18", 'ip-addresses':[['ip-address':'192.168.1.101', 'ip-address-type':'ipv4']]] ]
            ]
        ]
        Collection itemsToAdd = [cloudItem]

        // Mock for hostIdentitiesMap (parentServer lookup)
        def mockHostProjection = new ComputeServerIdentityProjection(id: 5L, externalId: "proxmox-node-1", name: "Proxmox Node 1", computeServerTypeCode: "proxmox-qemu-vm")
        mockComputeServerService.listIdentityProjections(mockCloud.id, null) >> Observable.just(mockHostProjection)


        List<ComputeServer> capturedVMs = []
        mockComputeServerService.bulkCreate(_ as List<ComputeServer>) >> { List<ComputeServer> vms ->
            capturedVMs.addAll(vms)
            return Observable.just(true) // Simulate successful save
        }

        when:
        vmSync.addMissingVirtualMachines(mockCloud, itemsToAdd)

        then: "Verify one VM was captured for creation"
        capturedVMs.size() == 1
        def newVM = capturedVMs[0]

        and: "Verify basic properties"
        newVM.externalId == "101"
        newVM.name == "test-vm-01"
        newVM.externalIp == "192.168.1.101"
        newVM.internalIp == "192.168.1.101"
        newVM.sshHost == "192.168.1.101"
        newVM.powerState == ComputeServer.PowerState.on
        newVM.maxMemory == 2147483648L
        newVM.maxCores == 2
        newVM.coresPerSocket == 1
        newVM.parentServer.id == 5L // Check if parent server was linked

        and: "Verify disk information"
        newVM.maxStorage == (20L * 1024L * 1024L * 1024L + 5L * 1024L * 1024L * 1024L) // 25GB total
        newVM.configMap != null
        newVM.configMap.proxmoxDisks == JsonOutput.toJson(cloudItem.disks)

        and: "Verify network interface information"
        newVM.configMap.proxmoxNics == JsonOutput.toJson(cloudItem.networkInterfaces)

        and: "Verify QEMU agent information"
        newVM.configMap.qemuAgentStatus == "running"
        newVM.configMap.qemuAgentData == JsonOutput.toJson(cloudItem.qemuAgent.data)
        newVM.configMap.qemuAgentRawInterfaces == JsonOutput.toJson(cloudItem.qemuAgent.networkInterfaces)

        and: "Verify OS information from agent"
        newVM.osType == "linux" // from qemuAgent.data.ostype
        newVM.serverOs.code == "ubuntu"
        newVM.serverOs.name == "Ubuntu"
    }

    // Add more tests for addMissingVirtualMachines with different cloudItem structures (e.g. agent not running, no disks)

    // Conceptual tests for updateMatchingVMs would follow a similar pattern:
    // 1. Create an existing ComputeServer object.
    // 2. Create a cloudItem map with new/changed data.
    // 3. Mock context.async.computeServer.bulkSave and other interactions.
    // 4. Call vmSync.updateMatchingVMs with an UpdateItem.
    // 5. Assert that the existingItem's properties and configMap are updated correctly.
    // 6. Assert that updateMachineMetrics is called with correct parameters.

    // Test for updateMachineMetrics itself could also be valuable if it had more complex logic.
}
