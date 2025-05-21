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
    // Conceptual tests for requestVMConsole might go here
}
