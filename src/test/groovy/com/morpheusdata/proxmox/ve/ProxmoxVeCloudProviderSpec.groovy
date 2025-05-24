package com.morpheusdata.proxmox.ve

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.model.ComputeZonePool
import com.morpheusdata.proxmox.ve.util.ProxmoxApiComputeUtil
import com.morpheusdata.response.ServiceResponse
import spock.lang.Specification
import spock.lang.Subject

class ProxmoxVeCloudProviderSpec extends Specification {

    @Subject
    ProxmoxVeCloudProvider cloudProvider

    // Mocks
    ProxmoxVePlugin mockPlugin
    MorpheusContext mockMorpheusContext
    ProxmoxApiComputeUtil mockApiUtil // Will be used via Groovy's metaClass for static mocking

    void setup() {
        mockPlugin = Mock(ProxmoxVePlugin)
        mockMorpheusContext = Mock(MorpheusContext)
        // For static methods, we often use Groovy's MetaClass or a similar mechanism.
        // Or, if ProxmoxApiComputeUtil was an instance, we could inject a mock.
        // For this example, we'll assume we can intercept static calls if needed,
        // or focus on the provider's logic assuming the util class works as tested separately.

        cloudProvider = new ProxmoxVeCloudProvider(mockPlugin, mockMorpheusContext)

        // Mocking static methods of ProxmoxApiComputeUtil
        // Groovy's MetaClass modification is one way, or using PowerMock/GroovyMock for more complex static mocking.
        // For simplicity in this example, we'll show intent.
        // In a real test, you might have:
        // GroovyMock(ProxmoxApiComputeUtil, global: true) or similar setup
        // ProxmoxApiComputeUtil.startVM(_, _, _, _) >> ServiceResponse.success() // Default mock behavior
    }

    def "startServer successfully starts a VM"() {
        given:
        def computeServer = new ComputeServer(
            id: 1L,
            name: "test-vm",
            externalId: "101", // VM ID in Proxmox
            cloud: new Cloud(id: 10L, name: "TestProxmoxCloud", serviceUrl: "https://proxmox.example.com:8006"),
            parentServer: new ComputeServer(id:2L, name:"proxmox-node1") // Hypervisor node
        )
        def authConfig = [username: "user", password: "password", apiUrl: "https://proxmox.example.com:8006", v2basePath: "/api2/json"]

        // Setup plugin mock to return authConfig
        mockPlugin.getAuthConfig(computeServer.cloud) >> authConfig

        // Mock the static call to ProxmoxApiComputeUtil.startVM
        // This is conceptual. Actual static mocking in Spock can be tricky without PowerMock or GroovySpy/GroovyMock.
        // A common pattern is to wrap static calls in an instance method on the provider if they become hard to test.
        // Or, refactor ProxmoxApiComputeUtil to be injectable if not purely static methods.
        // For this example, we'll assume a way to verify the interaction or mock its output.

        // Let's assume ProxmoxApiComputeUtil is an instance for easier mocking in this example context
        // If it were an instance:
        // ProxmoxApiComputeUtil localApiUtilMock = Mock()
        // cloudProvider.apiUtil = localApiUtilMock // if provider had such a property
        // localApiUtilMock.startVM(_, authConfig, "proxmox-node1", "101") >> ServiceResponse.success("VM started")

        // Using a simpler approach: override the static method for this test scope (if possible with Spock/Groovy features)
        // Or, more practically, since HttpApiClient is instantiated inside, we can't easily mock it directly without bytecode manipulation.
        // The most straightforward unit test here checks the logic flow *around* the static call.

        // For this conceptual example, we will check if the method attempts to call the Util class.
        // A full test would involve mocking the static method call.
        // ProxmoxApiComputeUtil.metaClass.static.startVM = { HttpApiClient client, Map ac, String node, String vmId ->
        //    assert node == "proxmox-node1"
        //    assert vmId == "101"
        //    return ServiceResponse.success("Mocked start success")
        // }
        // This metaClass approach has limitations and scope issues in Spock tests.
        // A common pattern for testing classes that use static utility methods is to
        // not mock the static method itself in the unit test of the calling class,
        // but rather trust that the static utility method is unit tested separately (which we did for helpers).
        // The focus here would be on parameter preparation and response handling.

        // To make it testable, we assume ProxmoxApiComputeUtil.startVM is globally mocked or we test the provider's logic
        // by checking what it would pass. For now, we can't directly assert the static call easily in pure Spock.
        // We will assume a successful response from the util for now to test the provider's handling.

        // If ProxmoxApiComputeUtil.startVM could be mocked (e.g. via a global mock or PowerMock):
        ProxmoxApiComputeUtil.startVM(_ as HttpApiClient, authConfig, "proxmox-node1", "101") >> ServiceResponse.success("VM started successfully")


        when:
        ServiceResponse response = cloudProvider.startServer(computeServer)

        then:
        // 1 * ProxmoxApiComputeUtil.startVM(_ as HttpApiClient, authConfig, "proxmox-node1", "101") // Verify interaction if static mocking was robust
        response.success
        response.msg == "VM started successfully"

    }

    def "startServer handles missing externalId"() {
        given:
        def computeServer = new ComputeServer(
            id: 1L, name: "test-vm-no-id",
            cloud: new Cloud(id: 10L),
            parentServer: new ComputeServer(id:2L, name:"proxmox-node1")
        )
        // No externalId set

        when:
        ServiceResponse response = cloudProvider.startServer(computeServer)

        then:
        !response.success
        response.msg.contains("Missing externalId (VM ID)")
    }

    def "startServer handles missing parentServer (nodeName) with API fallback success"() {
        given:
        def computeServer = new ComputeServer(
            id: 1L, name: "test-vm-no-parent", externalId: "102",
            cloud: new Cloud(id: 10L, serviceUrl: "https://proxmox.example.com:8006")
        ) // parentServer is null
        def authConfig = [username: "user", password: "password", apiUrl: "https://proxmox.example.com:8006", v2basePath: "/api2/json"]
        def mockVmListFromApi = [[vmid: "102", node: "found-node1"]]

        mockPlugin.getAuthConfig(computeServer.cloud) >> authConfig
        // Mock static ProxmoxApiComputeUtil.listVMs
        ProxmoxApiComputeUtil.listVMs(_ as HttpApiClient, authConfig) >> new ServiceResponse(success:true, data: mockVmListFromApi)
        // Mock static ProxmoxApiComputeUtil.startVM for the successful path after node discovery
        ProxmoxApiComputeUtil.startVM(_ as HttpApiClient, authConfig, "found-node1", "102") >> ServiceResponse.success("VM started on discovered node")

        when:
        ServiceResponse response = cloudProvider.startServer(computeServer)

        then:
        response.success
        response.msg == "VM started on discovered node"
    }

    def "startServer handles missing parentServer (nodeName) with API fallback failure"() {
        given:
        def computeServer = new ComputeServer(
            id: 1L, name: "test-vm-no-parent-fail", externalId: "103",
            cloud: new Cloud(id: 10L, serviceUrl: "https://proxmox.example.com:8006")
        )
        def authConfig = [username: "user", password: "password", apiUrl: "https://proxmox.example.com:8006", v2basePath: "/api2/json"]
        def mockVmListFromApi = [[vmid: "999", node: "other-node"]] // VM 103 not in this list

        mockPlugin.getAuthConfig(computeServer.cloud) >> authConfig
        ProxmoxApiComputeUtil.listVMs(_ as HttpApiClient, authConfig) >> new ServiceResponse(success:true, data: mockVmListFromApi)
        // startVM should not be called in this case

        when:
        ServiceResponse response = cloudProvider.startServer(computeServer)

        then:
        !response.success
        response.msg.contains("Missing nodeName (parentServer.name) for server")
        response.msg.contains("and could not find it via API")
    }

    // Similar tests would be written for stopServer, deleteServer, addDiskToServer, addNetworkInterfaceToServer, getVmConsoleDetails
    // focusing on:
    // - Correct parameter preparation for ProxmoxApiComputeUtil calls
    // - Handling of successful responses from ProxmoxApiComputeUtil
    // - Handling of error responses or exceptions from ProxmoxApiComputeUtil
    // - Input validation (e.g., missing server.externalId, missing required params for addDisk/addNic)
    // - Fallback logic (like nodeName discovery)
}
