package com.morpheusdata.proxmox.ve.util

import spock.lang.Specification
import spock.lang.Unroll

class ProxmoxApiComputeUtilSpec extends Specification {

    def "setup"() {
        // No setup needed for static methods
    }

    @Unroll
    def "parseSizeToBytes converts '#sizeStr' to #expectedBytes bytes"() {
        when:
        long result = ProxmoxApiComputeUtil.parseSizeToBytes(sizeStr)

        then:
        result == expectedBytes

        where:
        sizeStr    | expectedBytes
        "32G"      | 32L * 1024L * 1024L * 1024L
        "1024M"    | 1024L * 1024L * 1024L
        "500K"     | 500L * 1024L
        "1T"       | 1L * 1024L * 1024L * 1024L * 1024L
        "1P"       | 1L * 1024L * 1024L * 1024L * 1024L * 1024L
        "1024"     | 1024L // Assumes bytes if no unit
        "0G"       | 0L
        "0"        | 0L
        null       | 0L
        ""         | 0L
        "100X"     | 0L // Invalid unit
        "garbage"  | 0L // Invalid format
        "100GB"    | 0L // common but not matching current regex, assumes single char unit
    }

    @Unroll
    def "parseDiskEntry correctly parses '#diskValue'"() {
        given:
        // Optional: mock logging if it becomes noisy or needs verification
        // GroovyMock(Slf4j, global: true)

        when:
        Map result = ProxmoxApiComputeUtil.parseDiskEntry(diskKey, diskValue)

        then:
        result == expectedMap

        where:
        diskKey   | diskValue                                                  | expectedMap
        "virtio0" | "local-lvm:vm-102-disk-0,size=32G,iothread=1"              | [name: "virtio0", type: "virtio", storage: "local-lvm", file: "local-lvm:vm-102-disk-0", sizeRaw: "32G", sizeBytes: 32L * 1024L * 1024L * 1024L, format: "raw"] // Assuming default format raw if not specified
        "scsi1"   | "pve-storage:iso/ubuntu-20.04.iso,media=cdrom,size=1024M"  | [name: "scsi1", type: "scsi", storage: "pve-storage", file: "pve-storage:iso/ubuntu-20.04.iso", media: "cdrom", sizeRaw: "1024M", sizeBytes: 1024L * 1024L * 1024L, isCdRom: true]
        "ide0"    | "another-store:backup/vm-100.vma.gz,size=0"                | [name: "ide0", type: "ide", storage: "another-store", file: "another-store:backup/vm-100.vma.gz", sizeRaw: "0", sizeBytes: 0L, format: "raw"]
        "sata2"   | "ceph-store:rbd-image-123,size=100G,format=qcow2,ssd=1"    | [name: "sata2", type: "sata", storage: "ceph-store", file: "ceph-store:rbd-image-123", sizeRaw: "100G", sizeBytes: 100L * 1024L * 1024L * 1024L, format: "qcow2"]
        "virtio3" | "local-zfs:subvol-105-disk-0,size=64G"                     | [name: "virtio3", type: "virtio", storage: "local-zfs", file: "local-zfs:subvol-105-disk-0", sizeRaw: "64G", sizeBytes: 64L * 1024L * 1024L * 1024L, format: "raw"]
        "scsi0"   | "local:iso/alpine.iso,media=cdrom,size=123456K"            | [name: "scsi0", type: "scsi", storage: "local", file: "local:iso/alpine.iso", media: "cdrom", sizeRaw: "123456K", sizeBytes: 123456L * 1024L, isCdRom: true]
        "virtio1" | "local-lvm:vm-101-disk-1,size=16G,backup=0,iothread=1,ssd=1" | [name: "virtio1", type: "virtio", storage: "local-lvm", file: "local-lvm:vm-101-disk-1", sizeRaw: "16G", sizeBytes: 16L * 1024L * 1024L * 1024L, format: "raw"]
        "ide2"    | "local:120/vm-120-disk-1.qcow2,size=50G,format=qcow2"       | [name: "ide2", type: "ide", storage: "local", file: "local:120/vm-120-disk-1.qcow2", sizeRaw: "50G", sizeBytes: 50L * 1024L * 1024L * 1024L, format: "qcow2"]
        // Edge case: no size (should default to 0 or handle gracefully based on implementation)
        "sata0"   | "empty-storage:nodisk"                                     | [name: "sata0", type: "sata", storage: "empty-storage", file: "empty-storage:nodisk", format: "raw"] // sizeBytes might be 0 if size is missing
        // Edge case: only storage name (e.g. unused drive)
        "scsi2"   | "some_storage"                                             | [name: "scsi2", type: "scsi", storage: "some_storage", file: "some_storage", format: "raw"]
        // Malformed
        "scsi3"   | "local-lvm:size=32G"                                       | null // Or specific error map if preferred, current impl returns null on error
    }

    @Unroll
    def "parseNetworkInterfaceEntry correctly parses '#nicValue'"() {
        given:
        // Optional: mock logging

        when:
        Map result = ProxmoxApiComputeUtil.parseNetworkInterfaceEntry(nicKey, nicValue)

        then:
        result == expectedMap

        where:
        nicKey | nicValue                                                | expectedMap
        "net0" | "virtio=AA:BB:CC:DD:EE:FF,bridge=vmbr0,tag=100,firewall=1" | [name: "net0", model: "virtio", macAddress: "AA:BB:CC:DD:EE:FF", bridge: "vmbr0", vlanTag: "100", firewall: true]
        "net1" | "e1000=11:22:33:44:55:66,bridge=vmbr1"                   | [name: "net1", model: "e1000", macAddress: "11:22:33:44:55:66", bridge: "vmbr1"]
        "net2" | "rtl8139=00:11:22:33:44:55,bridge=vmbr0,firewall=0"       | [name: "net2", model: "rtl8139", macAddress: "00:11:22:33:44:55", bridge: "vmbr0", firewall: false] // Assuming firewall=0 means false
        "net3" | "vmxnet3=DE:AD:BE:EF:CA:FE,bridge=vmbr2,tag=205"          | [name: "net3", model: "vmxnet3", macAddress: "DE:AD:BE:EF:CA:FE", bridge: "vmbr2", vlanTag: "205"]
        // Case where model might be implied or MAC is missing (less common for Proxmox QEMU, but testing robustness)
        "net4" | "vmbr0"                                                 | [name: "net4", model: "vmbr0"] // Current parser might put bridge name in model if no '='
        "net5" | "virtio=,bridge=vmbr0"                                  | [name: "net5", model: "virtio", macAddress: "", bridge: "vmbr0"] // Empty MAC
        "net6" | "e1000=AB:BC:CD:DE:EF:F0"                                 | [name: "net6", model: "e1000", macAddress: "AB:BC:CD:DE:EF:F0"] // No bridge
        // Malformed
        "net7" | "bridgeonly"                                            | [name: "net7", model: "bridgeonly"]
        "net8" | "=AA:BB:CC:DD:EE:FF,bridge=vmbr0"                       | null // Malformed, model name is empty
    }

    @Unroll
    def "findPrimaryIpFromAgentNics correctly finds IP from agent NICs data"() {
        given:
        // No external dependencies to mock for this static helper

        when:
        String result = ProxmoxApiComputeUtil.findPrimaryIpFromAgentNics(agentNicsData)

        then:
        result == expectedIp

        where:
        description                                 | agentNicsData                                                                                                                                                             | expectedIp
        "single NIC, single IPv4"                   | [[name: "eth0", 'hardware-address': "AA:BB:CC:DD:EE:FF", 'ip-addresses': [['ip-address-type': 'ipv4', 'ip-address': '192.168.1.100', prefix: 24]]]]                               | "192.168.1.100"
        "multiple NICs, first valid IPv4"           | [[name: "eth0", 'ip-addresses': [['ip-address-type': 'ipv6', 'ip-address': 'fe80::1']]], [name: "eth1", 'ip-addresses': [['ip-address-type': 'ipv4', 'ip-address': '10.0.0.10', prefix: 8]]]] | "10.0.0.10"
        "NIC with multiple IPs, picks first IPv4"   | [[name: "eth0", 'ip-addresses': [['ip-address-type': 'ipv6', 'ip-address': 'fe80::1'], ['ip-address-type': 'ipv4', 'ip-address': '192.168.1.101'], ['ip-address-type': 'ipv4', 'ip-address': '192.168.1.102']]]] | "192.168.1.101"
        "localhost IPv4 present, should be ignored" | [[name: "lo", 'ip-addresses': [['ip-address-type': 'ipv4', 'ip-address': '127.0.0.1']]], [name: "eth0", 'ip-addresses': [['ip-address-type': 'ipv4', 'ip-address': '192.168.1.103']]]] | "192.168.1.103"
        "only localhost IPv4"                       | [[name: "lo", 'ip-addresses': [['ip-address-type': 'ipv4', 'ip-address': '127.0.0.1']]]]                                                                                        | null // or "127.0.0.1" if current logic allows, but ideally null
        "only IPv6 addresses"                       | [[name: "eth0", 'ip-addresses': [['ip-address-type': 'ipv6', 'ip-address': '2001:db8::1']]]]                                                                                     | null
        "no IP addresses on NIC"                    | [[name: "eth0", 'hardware-address': "AA:BB:CC:DD:EE:00", 'ip-addresses': []]]                                                                                                    | null
        "NIC data is null"                          | null                                                                                                                                                                              | null
        "NIC data is empty list"                    | []                                                                                                                                                                                | null
        "NIC entry without 'ip-addresses' key"      | [[name: "eth0", 'hardware-address': "AA:BB:CC:DD:EE:01"]]                                                                                                                         | null
        "IP info without 'ip-address' key"          | [[name: "eth0", 'ip-addresses': [['ip-address-type': 'ipv4', prefix: 24]]]]                                                                                                     | null // This case should not happen with real agent data but good for robustness
    }

    // Conceptual tests for addVMDisk/addVMNetworkInterface (index finding) might go here if refactored

    def "test requestVMConsole vnc success"() {
        given:
        def mockClient = Mock(HttpApiClient)
        def authConfig = [apiUrl: "https://localhost:8006", v2basePath: "/api2/json", username: "user", password: "password"]
        def mockTokenResponse = new ServiceResponse(success: true, data: [token: "faketoken", csrfToken: "fakecsrftoken"])
        ProxmoxApiComputeUtil.metaClass.static.getApiV2Token = { Map ac -> mockTokenResponse }
        def consoleDetails = [user: 'root@pam', ticket: 'ticket123', port: '5900', host: '::1']
        def mockApiResponse = new ServiceResponse(success: true, data: [data: consoleDetails])
        mockClient.callJsonApi(
            authConfig.apiUrl,
            "/api2/json/nodes/node1/qemu/200/vncproxy",
            null,
            null,
            { it.headers['Cookie'] == "PVEAuthCookie=faketoken" && it.headers['CSRFPreventionToken'] == "fakecsrftoken" && it.body == [:] },
            'POST'
        ) >> mockApiResponse

        when:
        def response = ProxmoxApiComputeUtil.requestVMConsole(mockClient, authConfig, 'node1', '200', 'vnc')

        then:
        response.success
        response.data.ticket == 'ticket123'
        response.data.type == 'vnc'
        response.data.proxmoxHost == 'localhost'
        response.data.proxmoxPort == 8006
    }

    def "test requestVMConsole unsupported type"() {
        given:
        def mockClient = Mock(HttpApiClient)
        def authConfig = [apiUrl: "https://localhost:8006", v2basePath: "/api2/json", username: "user", password: "password"]
        def mockTokenResponse = new ServiceResponse(success: true, data: [token: "faketoken", csrfToken: "fakecsrftoken"])
        ProxmoxApiComputeUtil.metaClass.static.getApiV2Token = { Map ac -> mockTokenResponse }

        when:
        def response = ProxmoxApiComputeUtil.requestVMConsole(mockClient, authConfig, 'node1', '200', 'spice')

        then:
        !response.success
        response.msg.contains('Unsupported console type')
        0 * mockClient.callJsonApi(_, _, _, _, _, _)
    }

    def "test requestVMConsole token failure"() {
        given:
        def mockClient = Mock(HttpApiClient)
        def authConfig = [apiUrl: "https://localhost:8006", v2basePath: "/api2/json", username: "user", password: "password"]
        def mockTokenResponse = new ServiceResponse(success: false, msg: "bad auth")
        ProxmoxApiComputeUtil.metaClass.static.getApiV2Token = { Map ac -> mockTokenResponse }

        when:
        def response = ProxmoxApiComputeUtil.requestVMConsole(mockClient, authConfig, 'node1', '200', 'vnc')

        then:
        !response.success
        response.msg.contains('Failed to get API token')
        0 * mockClient.callJsonApi(_, _, _, _, _, _)
    }

    def "test requestVMConsole API error"() {
        given:
        def mockClient = Mock(HttpApiClient)
        def authConfig = [apiUrl: "https://localhost:8006", v2basePath: "/api2/json", username: "user", password: "password"]
        def mockTokenResponse = new ServiceResponse(success: true, data: [token: "faketoken", csrfToken: "fakecsrftoken"])
        ProxmoxApiComputeUtil.metaClass.static.getApiV2Token = { Map ac -> mockTokenResponse }
        def mockApiResponse = new ServiceResponse(success: false, msg: "API failure")
        mockClient.callJsonApi(
            authConfig.apiUrl,
            "/api2/json/nodes/node1/qemu/200/vncproxy",
            null,
            null,
            { it.headers['Cookie'] == "PVEAuthCookie=faketoken" && it.headers['CSRFPreventionToken'] == "fakecsrftoken" && it.body == [:] },
            'POST'
        ) >> mockApiResponse

        when:
        def response = ProxmoxApiComputeUtil.requestVMConsole(mockClient, authConfig, 'node1', '200', 'vnc')

        then:
        !response.success
        response.msg.contains('API failure')
    }

    // Snapshot Management Tests
    def "test listSnapshots success"() {
        given:
        def mockClient = Mock(HttpApiClient)
        def authConfig = [apiUrl: "https://localhost:8006", v2basePath: "/api2/json", username: "user", password: "password"]
        def mockTokenResponse = new ServiceResponse(success: true, data: [token: "faketoken", csrfToken: "fakecsrftoken"])
        
        // Groovy's metaClass programming to mock static method
        ProxmoxApiComputeUtil.metaClass.static.getApiV2Token = { Map ac -> mockTokenResponse }

        def snapshotList = [[name: "snap1", description: "Test Snapshot 1"], [name: "snap2", description: "Test Snapshot 2"]]
        // callListApiV2 returns a ServiceResponse where the actual data is in response.data
        def mockApiResponse = new ServiceResponse(success: true, data: snapshotList) 
        // This mocks the callJsonApi *inside* callListApiV2, not listSnapshots directly calling callJsonApi
        mockClient.callJsonApi(authConfig.apiUrl, "/api2/json/nodes/node1/qemu/100/snapshot", null, null, _ as HttpApiClient.RequestOptions, 'GET') >> mockApiResponse

        when:
        def response = ProxmoxApiComputeUtil.listSnapshots(mockClient, authConfig, "node1", "100")

        then:
        response.success
        response.data.size() == 2
        response.data[0].name == "snap1"
        response.data[1].description == "Test Snapshot 2"
        // Verification of callJsonApi happens implicitly via the setup of mockClient interaction above
    }

    def "test listSnapshots API error"() {
        given:
        def mockClient = Mock(HttpApiClient)
        def authConfig = [apiUrl: "https://localhost:8006", v2basePath: "/api2/json", username: "user", password: "password"]
        def mockTokenResponse = new ServiceResponse(success: true, data: [token: "faketoken", csrfToken: "fakecsrftoken"])
        ProxmoxApiComputeUtil.metaClass.static.getApiV2Token = { Map ac -> mockTokenResponse }
        
        def mockApiResponse = new ServiceResponse(success: false, msg: "API Error")
        mockClient.callJsonApi(authConfig.apiUrl, "/api2/json/nodes/node1/qemu/100/snapshot", null, null, _ as HttpApiClient.RequestOptions, 'GET') >> mockApiResponse

        when:
        def response = ProxmoxApiComputeUtil.listSnapshots(mockClient, authConfig, "node1", "100")

        then:
        !response.success
        response.msg.contains("API Error")
    }
    
    def "test createSnapshot success"() {
        given:
        def mockClient = Mock(HttpApiClient)
        def authConfig = [apiUrl: "https://localhost:8006", v2basePath: "/api2/json", username: "user", password: "password"]
        def mockTokenResponse = new ServiceResponse(success: true, data: [token: "faketoken", csrfToken: "fakecsrftoken"])
        ProxmoxApiComputeUtil.metaClass.static.getApiV2Token = { Map ac -> mockTokenResponse }

        def mockApiResponse = new ServiceResponse(success: true, data: [data: "UPID:node1:000ABC:taskid:snapname:user@realm:"]) // Proxmox returns task ID in data.data
        
        when:
        def response = ProxmoxApiComputeUtil.createSnapshot(mockClient, authConfig, "node1", "101", "new_snap", "A new snapshot")

        then:
        response.success
        response.data.taskId.contains("UPID:node1:000ABC")
        1 * mockClient.callJsonApi(
            authConfig.apiUrl,
            "/api2/json/nodes/node1/qemu/101/snapshot",
            null,
            null,
            { it.body.snapname == "new_snap" && it.body.description == "A new snapshot" && it.headers['Cookie'] == "PVEAuthCookie=faketoken" },
            'POST'
        )
    }

    def "test createSnapshot API error"() {
        given:
        def mockClient = Mock(HttpApiClient)
        def authConfig = [apiUrl: "https://localhost:8006", v2basePath: "/api2/json", username: "user", password: "password"]
        def mockTokenResponse = new ServiceResponse(success: true, data: [token: "faketoken", csrfToken: "fakecsrftoken"])
        ProxmoxApiComputeUtil.metaClass.static.getApiV2Token = { Map ac -> mockTokenResponse }

        def mockApiResponse = new ServiceResponse(success: false, msg: "Failed to create snapshot")
        mockClient.callJsonApi(_, _, _, _, _, 'POST') >> mockApiResponse
        
        when:
        def response = ProxmoxApiComputeUtil.createSnapshot(mockClient, authConfig, "node1", "101", "new_snap", "A new snapshot")

        then:
        !response.success
        response.msg.contains("Failed to create snapshot")
    }

    def "test deleteSnapshot success"() {
        given:
        def mockClient = Mock(HttpApiClient)
        def authConfig = [apiUrl: "https://localhost:8006", v2basePath: "/api2/json", username: "user", password: "password"]
        def mockTokenResponse = new ServiceResponse(success: true, data: [token: "faketoken", csrfToken: "fakecsrftoken"])
        ProxmoxApiComputeUtil.metaClass.static.getApiV2Token = { Map ac -> mockTokenResponse }
        def mockApiResponse = new ServiceResponse(success: true, data: [data: "UPID:node1:000DEF:taskid:snapname:user@realm:"])

        when:
        def response = ProxmoxApiComputeUtil.deleteSnapshot(mockClient, authConfig, "node1", "102", "snap_to_delete")

        then:
        response.success
        response.data.taskId.contains("UPID:node1:000DEF")
        1 * mockClient.callJsonApi(
            authConfig.apiUrl,
            "/api2/json/nodes/node1/qemu/102/snapshot/snap_to_delete",
            null,
            null,
            { it.headers['Cookie'] == "PVEAuthCookie=faketoken" },
            'DELETE'
        )
    }

    def "test rollbackSnapshot success"() {
        given:
        def mockClient = Mock(HttpApiClient)
        def authConfig = [apiUrl: "https://localhost:8006", v2basePath: "/api2/json", username: "user", password: "password"]
        def mockTokenResponse = new ServiceResponse(success: true, data: [token: "faketoken", csrfToken: "fakecsrftoken"])
        ProxmoxApiComputeUtil.metaClass.static.getApiV2Token = { Map ac -> mockTokenResponse }
        def mockApiResponse = new ServiceResponse(success: true, data: [data: "UPID:node1:000GHI:taskid:snapname:user@realm:"])

        when:
        def response = ProxmoxApiComputeUtil.rollbackSnapshot(mockClient, authConfig, "node1", "103", "snap_to_rollback")

        then:
        response.success
        response.data.taskId.contains("UPID:node1:000GHI")
        1 * mockClient.callJsonApi(
            authConfig.apiUrl,
            "/api2/json/nodes/node1/qemu/103/snapshot/snap_to_rollback/rollback",
            null,
            null,
            { it.body == [:] && it.headers['Cookie'] == "PVEAuthCookie=faketoken" }, // Empty body for rollback
            'POST'
        )
    }

    // Teardown method to reset metaclass changes after each feature method
    def cleanup() {
        GroovySystem.metaClassRegistry.removeMetaClass(ProxmoxApiComputeUtil.class)
    }


    // removeVMDisk tests
    def "test removeVMDisk success"() {
        given:
        def mockClient = Mock(HttpApiClient)
        def authConfig = [apiUrl: "https://localhost:8006", v2basePath: "/api2/json", username: "user", password: "password"]
        def mockTokenResponse = new ServiceResponse(success: true, data: [token: "faketoken", csrfToken: "fakecsrftoken"])
        ProxmoxApiComputeUtil.metaClass.static.getApiV2Token = { Map ac -> mockTokenResponse }
        def mockApiResponse = new ServiceResponse(success: true, data: [data: "UPID:nodeX:taskXYZ:removeDisk:user@realm:"])

        when:
        def response = ProxmoxApiComputeUtil.removeVMDisk(mockClient, authConfig, "nodeX", "105", "scsi1")

        then:
        response.success
        response.data.taskId.contains("UPID:nodeX:taskXYZ")
        1 * mockClient.callJsonApi(
            authConfig.apiUrl,
            "/api2/json/nodes/nodeX/qemu/105/config",
            null,
            null,
            { it.body.delete == "scsi1" && it.headers['Cookie'] == "PVEAuthCookie=faketoken" },
            'POST'
        )
    }

    def "test removeVMDisk API error"() {
        given:
        def mockClient = Mock(HttpApiClient)
        def authConfig = [apiUrl: "https://localhost:8006", v2basePath: "/api2/json", username: "user", password: "password"]
        def mockTokenResponse = new ServiceResponse(success: true, data: [token: "faketoken", csrfToken: "fakecsrftoken"])
        ProxmoxApiComputeUtil.metaClass.static.getApiV2Token = { Map ac -> mockTokenResponse }
        def mockApiResponse = new ServiceResponse(success: false, msg: "Disk not found or other error")
        mockClient.callJsonApi(_, _, _, _, _, 'POST') >> mockApiResponse

        when:
        def response = ProxmoxApiComputeUtil.removeVMDisk(mockClient, authConfig, "nodeX", "105", "scsi1")

        then:
        !response.success
        response.msg.contains("Disk not found or other error")
    }

    // removeVMNetworkInterface tests
    def "test removeVMNetworkInterface success"() {
        given:
        def mockClient = Mock(HttpApiClient)
        def authConfig = [apiUrl: "https://localhost:8006", v2basePath: "/api2/json", username: "user", password: "password"]
        def mockTokenResponse = new ServiceResponse(success: true, data: [token: "faketoken", csrfToken: "fakecsrftoken"])
        ProxmoxApiComputeUtil.metaClass.static.getApiV2Token = { Map ac -> mockTokenResponse }
        def mockApiResponse = new ServiceResponse(success: true, data: [data: "UPID:nodeY:taskABC:removeNet:user@realm:"])

        when:
        def response = ProxmoxApiComputeUtil.removeVMNetworkInterface(mockClient, authConfig, "nodeY", "106", "net0")

        then:
        response.success
        response.data.taskId.contains("UPID:nodeY:taskABC")
        1 * mockClient.callJsonApi(
            authConfig.apiUrl,
            "/api2/json/nodes/nodeY/qemu/106/config",
            null,
            null,
            { it.body.delete == "net0" && it.headers['Cookie'] == "PVEAuthCookie=faketoken" },
            'POST'
        )
    }

    // updateVMNetworkInterface tests
    def "test updateVMNetworkInterface success"() {
        given:
        def mockClient = Mock(HttpApiClient)
        def authConfig = [apiUrl: "https://localhost:8006", v2basePath: "/api2/json", username: "user", password: "password"]
        def mockTokenResponse = new ServiceResponse(success: true, data: [token: "faketoken", csrfToken: "fakecsrftoken"])
        ProxmoxApiComputeUtil.metaClass.static.getApiV2Token = { Map ac -> mockTokenResponse }
        def mockApiResponse = new ServiceResponse(success: true, data: [data: "UPID:nodeZ:task123:updateNet:user@realm:"])

        when:
        def response = ProxmoxApiComputeUtil.updateVMNetworkInterface(mockClient, authConfig, "nodeZ", "107", "net1", "vmbr1", "e1000", "200", true)

        then:
        response.success
        response.data.taskId.contains("UPID:nodeZ:task123")
        1 * mockClient.callJsonApi(
            authConfig.apiUrl,
            "/api2/json/nodes/nodeZ/qemu/107/config",
            null,
            null,
            { it.body.net1 == "model=e1000,bridge=vmbr1,tag=200,firewall=1" && it.headers['Cookie'] == "PVEAuthCookie=faketoken" },
            'POST'
        )
    }
    
    def "test updateVMNetworkInterface no vlan no firewall"() {
        given:
        def mockClient = Mock(HttpApiClient)
        def authConfig = [apiUrl: "https://localhost:8006", v2basePath: "/api2/json", username: "user", password: "password"]
        def mockTokenResponse = new ServiceResponse(success: true, data: [token: "faketoken", csrfToken: "fakecsrftoken"])
        ProxmoxApiComputeUtil.metaClass.static.getApiV2Token = { Map ac -> mockTokenResponse }
        def mockApiResponse = new ServiceResponse(success: true, data: [data: "UPID:nodeZ:task456:updateNet:user@realm:"])

        when:
        def response = ProxmoxApiComputeUtil.updateVMNetworkInterface(mockClient, authConfig, "nodeZ", "108", "net0", "vmbr0", "virtio", null, false)

        then:
        response.success
        response.data.taskId.contains("UPID:nodeZ:task456")
        1 * mockClient.callJsonApi(
            authConfig.apiUrl,
            "/api2/json/nodes/nodeZ/qemu/108/config",
            null,
            null,
            { it.body.net0 == "model=virtio,bridge=vmbr0,firewall=0" && it.headers['Cookie'] == "PVEAuthCookie=faketoken" },
            'POST'
        )
    }

    // resizeVMCompute tests
    def "test resizeVMCompute only CPU and RAM"() {
        given:
        def mockClient = Mock(HttpApiClient)
        def authConfig = [apiUrl: "https://localhost:8006", v2basePath: "/api2/json", username: "user", password: "password"]
        def mockTokenResponse = new ServiceResponse(success: true, data: [token: "faketoken", csrfToken: "fakecsrftoken"])
        ProxmoxApiComputeUtil.metaClass.static.getApiV2Token = { Map ac -> mockTokenResponse }
        def mockApiResponse = new ServiceResponse(success: true, data: [data: "UPID:nodeA:task789:resize:user@realm:"])

        when:
        def response = ProxmoxApiComputeUtil.resizeVMCompute(mockClient, authConfig, "nodeA", "109", 4L, 8192L * 1024L * 1024L) // 8GB RAM

        then:
        response.success
        response.data.taskId.contains("UPID:nodeA:task789")
        1 * mockClient.callJsonApi(
            authConfig.apiUrl,
            "/api2/json/nodes/nodeA/qemu/109/config",
            null,
            null,
            { it.body.vcpus == 4L && it.body.memory == 8192L && !it.body.containsKey("net0") && it.headers['Cookie'] == "PVEAuthCookie=faketoken" },
            'POST'
        )
    }

     def "test resizeVMCompute only CPU"() {
        given:
        def mockClient = Mock(HttpApiClient)
        def authConfig = [apiUrl: "https://localhost:8006", v2basePath: "/api2/json", username: "user", password: "password"]
        def mockTokenResponse = new ServiceResponse(success: true, data: [token: "faketoken", csrfToken: "fakecsrftoken"])
        ProxmoxApiComputeUtil.metaClass.static.getApiV2Token = { Map ac -> mockTokenResponse }
        def mockApiResponse = new ServiceResponse(success: true, data: [data: "UPID:nodeB:task101:resize:user@realm:"])

        when:
        def response = ProxmoxApiComputeUtil.resizeVMCompute(mockClient, authConfig, "nodeB", "110", 2L, null)

        then:
        response.success
        response.data.taskId.contains("UPID:nodeB:task101")
        1 * mockClient.callJsonApi(
            authConfig.apiUrl,
            "/api2/json/nodes/nodeB/qemu/110/config",
            null,
            null,
            { it.body.vcpus == 2L && !it.body.containsKey("memory") && !it.body.containsKey("net0") },
            'POST'
        )
    }

    def "test resizeVMCompute only RAM"() {
        given:
        def mockClient = Mock(HttpApiClient)
        def authConfig = [apiUrl: "https://localhost:8006", v2basePath: "/api2/json", username: "user", password: "password"]
        def mockTokenResponse = new ServiceResponse(success: true, data: [token: "faketoken", csrfToken: "fakecsrftoken"])
        ProxmoxApiComputeUtil.metaClass.static.getApiV2Token = { Map ac -> mockTokenResponse }
        def mockApiResponse = new ServiceResponse(success: true, data: [data: "UPID:nodeC:task112:resize:user@realm:"])

        when:
        def response = ProxmoxApiComputeUtil.resizeVMCompute(mockClient, authConfig, "nodeC", "111", null, 4096L * 1024L * 1024L) // 4GB

        then:
        response.success
        response.data.taskId.contains("UPID:nodeC:task112")
        1 * mockClient.callJsonApi(
            authConfig.apiUrl,
            "/api2/json/nodes/nodeC/qemu/111/config",
            null,
            null,
            { it.body.memory == 4096L && !it.body.containsKey("vcpus") && !it.body.containsKey("net0") },
            'POST'
        )
    }

    def "test resizeVMCompute no parameters"() {
        given:
        def mockClient = Mock(HttpApiClient) // Not expected to be called
        def authConfig = [apiUrl: "https://localhost:8006", v2basePath: "/api2/json", username: "user", password: "password"]
        // Token might not even be fetched if parameters are checked first
        def mockTokenResponse = new ServiceResponse(success: true, data: [token: "faketoken", csrfToken: "fakecsrftoken"])
        ProxmoxApiComputeUtil.metaClass.static.getApiV2Token = { Map ac -> mockTokenResponse }


        when:
        def response = ProxmoxApiComputeUtil.resizeVMCompute(mockClient, authConfig, "nodeD", "112", null, null)

        then:
        !response.success
        response.msg.contains("No resize parameters (CPU or RAM) provided")
        0 * mockClient.callJsonApi(_, _, _, _, _, _) // Ensure no API call is made
    }
}
