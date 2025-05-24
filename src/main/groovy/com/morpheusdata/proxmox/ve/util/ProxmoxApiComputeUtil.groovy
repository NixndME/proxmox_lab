package com.morpheusdata.proxmox.ve.util

import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.response.ServiceResponse
import groovy.util.logging.Slf4j
import org.apache.http.entity.ContentType
import groovy.json.JsonSlurper
import com.morpheusdata.proxmox.ve.util.ProxmoxSslUtil
import com.morpheusdata.proxmox.ve.util.ProxmoxApiUtil


@Slf4j
class ProxmoxApiComputeUtil {

    //static final String API_BASE_PATH = "/api2/json"
    static final Long API_CHECK_WAIT_INTERVAL = 2000

    static {
        // Ensure any custom SSL configuration is applied once
        ProxmoxSslUtil.configureSslContextIfNeeded()
    }


    /*
    static setCloudInitData(HttpApiClient client, Map authConfig, String node, String vmId, String ciData) {
        log.debug("resizeVMCompute")


        try {
            def tokenCfg = getApiV2Token(authConfig.username, authConfig.password, authConfig.apiUrl).data
            def opts = [
                    headers  : [
                            'Content-Type'       : 'application/json',
                            'Cookie'             : "PVEAuthCookie=$tokenCfg.token",
                            'CSRFPreventionToken': tokenCfg.csrfToken
                    ],
                    body     : [
                            vmid: vmId,
                            node: node,
                            vcpus: cpu,
                            memory: ramValue
                    ],
                    contentType: ContentType.APPLICATION_JSON,
                    ignoreSSL: ProxmoxSslUtil.IGNORE_SSL
            ]

            log.debug("Setting VM Compute Size $vmId on node $node...")
            log.debug("POST body is: $opts.body")
            sleep(10000)
            def results = ProxmoxApiUtil.callJsonApiWithRetry(client, 
                    (String) authConfig.apiUrl,
                    "${authConfig.v2basePath}/nodes/$node/qemu/$vmId/config",
                    null, null,
                    new HttpApiClient.RequestOptions(opts),
                    'POST'
            )

            return results
        } catch (e) {
            log.error "Error Provisioning VM: ${e}", e
            return ServiceResponse.error("Error Provisioning VM: ${e}")
        }
    }
*/


    static resizeVMCompute(HttpApiClient client, Map authConfig, String node, String vmId, Long cpu, Long ram) {
        log.info("Resizing VM ${vmId} on node ${node} to CPU: ${cpu}, RAM: ${ram} bytes")
        Long ramValueMB = ram != null ? ram / 1024 / 1024 : null 

        try {
            ServiceResponse tokenCfgResponse = getApiV2Token(authConfig)
            if (!tokenCfgResponse.success) {
                // Propagate the error from getApiV2Token, which already includes details
                return tokenCfgResponse 
            }
            def tokenCfg = tokenCfgResponse.data

            def bodyPayload = [ vmid: vmId, node: node ]
            if (cpu != null) bodyPayload.vcpus = cpu
            if (ramValueMB != null) bodyPayload.memory = ramValueMB
            
            if (cpu == null && ramValueMB == null) {
                String msg = "No resize parameters (CPU or RAM) provided for VM ${vmId} on node ${node}."
                log.warn(msg)
                return ServiceResponse.error(msg)
            }

            def opts = [
                    headers  : [
                            'Content-Type'       : 'application/json',
                            'Cookie'             : "PVEAuthCookie=${tokenCfg.token}",
                            'CSRFPreventionToken': tokenCfg.csrfToken
                    ],
                    body     : bodyPayload,
                    contentType: ContentType.APPLICATION_JSON,
                    ignoreSSL: ProxmoxSslUtil.IGNORE_SSL
            ]

            String apiPath = "${authConfig.v2basePath}/nodes/$node/qemu/$vmId/config"
            log.debug("Setting VM Compute Size for VM ${vmId} on node ${node}. Path: ${authConfig.apiUrl}${apiPath}. Payload: ${bodyPayload}")

            def results = ProxmoxApiUtil.callJsonApiWithRetry(client, authConfig.apiUrl, apiPath, null, null, new HttpApiClient.RequestOptions(opts), 'POST')

            if (results.success && results.data?.data) { 
                log.info("Successfully initiated resize for VM ${vmId} on node ${node}. Task ID: ${results.data.data}")
                return ServiceResponse.success("VM resize initiated for ${vmId}. Task ID: ${results.data.data}", [taskId: results.data.data])
            } else if (results.success) { 
                 log.warn("Resize VM ${vmId} API call on node ${node} was successful but no task ID found in results.data.data. Response (first 200 chars): ${results.content?.take(200)}")
                 return ServiceResponse.success("VM resize for ${vmId} on node ${node} reported success but no task ID was returned.", results.data ?: [:])
            } else {
                return ProxmoxApiUtil.validateApiResponse(results, "Failed to resize VM ${vmId} on node ${node}")
            }
        } catch (e) {
            log.error("Exception resizing VM ${vmId} on node ${node}: ${e.message}", e) // Log includes exception 'e' for stack trace
            return ServiceResponse.error("Exception resizing VM ${vmId} on node ${node}: ${e.message}")
        }
    }


    static startVM(HttpApiClient client, Map authConfig, String nodeId, String vmId) {
        log.info("Starting VM ${vmId} on node ${nodeId}") // Changed from debug to info
        return actionVMStatus(client, authConfig, nodeId, vmId, "start")
    }

    static rebootVM(HttpApiClient client, Map authConfig, String nodeId, String vmId) {
        log.info("Rebooting VM ${vmId} on node ${nodeId}") // Changed from debug to info
        return actionVMStatus(client, authConfig, nodeId, vmId, "reboot")
    }

    static shutdownVM(HttpApiClient client, Map authConfig, String nodeId, String vmId) {
        log.info("Shutting down VM ${vmId} on node ${nodeId}") // Changed from debug to info
        return actionVMStatus(client, authConfig, nodeId, vmId, "shutdown")
    }

    static stopVM(HttpApiClient client, Map authConfig, String nodeId, String vmId) {
        log.info("Stopping VM ${vmId} on node ${nodeId}") // Changed from debug to info
        return actionVMStatus(client, authConfig, nodeId, vmId, "stop")
    }

    static resetVM(HttpApiClient client, Map authConfig, String nodeId, String vmId) {
        log.info("Resetting VM ${vmId} on node ${nodeId}") // Changed from debug to info
        return actionVMStatus(client, authConfig, nodeId, vmId, "reset")
    }


    static actionVMStatus(HttpApiClient client, Map authConfig, String nodeId, String vmId, String action) {
        log.info("Performing action '${action}' on VM ${vmId} on node ${nodeId}")
        try {
            ServiceResponse tokenCfgResponse = getApiV2Token(authConfig)
            if (!tokenCfgResponse.success) {
                // Propagate the error from getApiV2Token, which already includes details
                return tokenCfgResponse 
            }
            def tokenCfg = tokenCfgResponse.data
            def opts = [
                    headers  : [
                            'Content-Type'       : 'application/json',
                            'Cookie'             : "PVEAuthCookie=$tokenCfg.token",
                            'CSRFPreventionToken': tokenCfg.csrfToken
                    ],
                    body     : [
                            vmid: vmId,
                            node: nodeId
                    ],
                    contentType: ContentType.APPLICATION_JSON,
                    ignoreSSL: ProxmoxSslUtil.IGNORE_SSL
            ]

            String apiPath = "${authConfig.v2basePath}/nodes/$nodeId/qemu/$vmId/status/$action"
            log.debug("Action VM status POST path for action '${action}' on VM ${vmId}, node ${nodeId}: ${authConfig.apiUrl}${apiPath}")
            def results = ProxmoxApiUtil.callJsonApiWithRetry(client, authConfig.apiUrl, apiPath, null, null, new HttpApiClient.RequestOptions(opts), 'POST')

            if (results.success && results.data?.data) { 
                log.info("Successfully initiated action '${action}' for VM ${vmId} on node ${nodeId}. Task ID: ${results.data.data}")
                return ServiceResponse.success("Action '${action}' initiated for VM ${vmId}. Task ID: ${results.data.data}", [taskId: results.data.data])
            } else if (results.success) { 
                log.warn("Action '${action}' for VM ${vmId} on node ${nodeId} was successful but no task ID found in results.data.data. Response (first 200 chars): ${results.content?.take(200)}")
                return ServiceResponse.success("Action '${action}' for VM ${vmId} on node ${nodeId} reported success but no task ID was returned.", results.data ?: [:])
            } else {
                return ProxmoxApiUtil.validateApiResponse(results, "Failed to perform action '${action}' on VM ${vmId} on node ${nodeId}")
            }
        } catch (e) {
            log.error("Exception performing action '${action}' on VM ${vmId} on node ${nodeId}: ${e.message}", e)
            return ServiceResponse.error("Exception performing action '${action}' on VM ${vmId} on node ${nodeId}: ${e.message}")
        }
    }


    static destroyVM(HttpApiClient client, Map authConfig, String nodeId, String vmId) {
        log.info("Destroying VM ${vmId} on node ${nodeId}")
        try {
            ServiceResponse tokenCfgResponse = getApiV2Token(authConfig)
            if (!tokenCfgResponse.success) {
                // Propagate the error from getApiV2Token, which already includes details
                return tokenCfgResponse 
            }
            def tokenCfg = tokenCfgResponse.data
            def opts = [
                    headers  : [
                            'Content-Type'       : 'application/json',
                            'Cookie'             : "PVEAuthCookie=$tokenCfg.token",
                            'CSRFPreventionToken': tokenCfg.csrfToken
                    ],
                    body: null,
                    ignoreSSL: ProxmoxSslUtil.IGNORE_SSL,
                    contentType: ContentType.APPLICATION_JSON,
            ]

            log.debug("Delete Opts: $opts")
            log.debug("Delete path is: $authConfig.apiUrl${authConfig.v2basePath}/nodes/$nodeId/qemu/$vmId/")

            def results = ProxmoxApiUtil.callJsonApiWithRetry(client,
                    authConfig.apiUrl,
                    "${authConfig.v2basePath}/nodes/$nodeId/qemu/$vmId/",
                    null,
                    null,
                    new HttpApiClient.RequestOptions(opts),
                    'DELETE')

            log.debug("VM Delete Response Details: ${results.toMap()}")
            return ProxmoxApiUtil.validateApiResponse(results, "Failed to destroy VM ${vmId} on node ${nodeId}")

        //TODO - check for non 200 response
        } catch (e) {
            log.error "Error Destroying VM: ${e}", e
            return ServiceResponse.error("Error Destroying VM: ${e}")
        }
    }


    static createImageTemplate(HttpApiClient client, Map authConfig, String imageName, String nodeId, int cpu, Long ram, String sourceUri = null) {
        log.info("Creating image template '${imageName}' on node ${nodeId} with cpu: ${cpu}, ram: ${ram}")
        ServiceResponse nextIdResponse = callListApiV2(client, "cluster/nextid", authConfig)
        if (!nextIdResponse.success || nextIdResponse.data == null) {
            log.error("Failed to get next VM ID for image template ${imageName}: ${nextIdResponse.msg}")
            return ServiceResponse.error("Failed to get next VM ID for image template ${imageName}: ${nextIdResponse.msg}")
        }
        def nextId = nextIdResponse.data.toString() // Ensure it's a string
        log.debug("Next VM Id for image template ${imageName} is: $nextId")
        def rtn = new ServiceResponse(success: false) // Default to success: false

        try {
            ServiceResponse tokenCfgResponse = getApiV2Token(authConfig)
            if (!tokenCfgResponse.success) {
                return tokenCfgResponse // Propagate error from getApiV2Token
            }
            def tokenCfg = tokenCfgResponse.data
            
            rtn.data = [:] // Initialize data as a map
            def opts = [
                    headers  : [
                            'Content-Type'       : 'application/json',
                            'Cookie'             : "PVEAuthCookie=$tokenCfg.token",
                            'CSRFPreventionToken': tokenCfg.csrfToken
                    ],
                    body     : [
                            vmid: nextId,
                            node: nodeId,
                            name: imageName,
                            template: true
                    ],
                    contentType: ContentType.APPLICATION_JSON,
                    ignoreSSL: ProxmoxSslUtil.IGNORE_SSL
            ]

            log.debug("Creating blank template for attaching qcow2...")
            log.debug("Path is: $authConfig.apiUrl${authConfig.v2basePath}/nodes/$nodeId/qemu/")
            def results = ProxmoxApiUtil.callJsonApiWithRetry(
                    client,
                    authConfig.apiUrl,
                    "${authConfig.v2basePath}/nodes/$nodeId/qemu/",
                    null,
                    null,
                    new HttpApiClient.RequestOptions(opts),
                    'POST'
            )

            if (results?.content) {
                def resultData = new JsonSlurper().parseText(results.content) // resultData is likely the task ID string
                if (results.success && resultData) {
                    rtn.success = true
                    // Proxmox API for creating a VM/template returns the task ID as the direct data.
                    rtn.data = [taskId: resultData, templateId: nextId] 
                    log.info("Successfully initiated creation of image template ${imageName} (VMID ${nextId}). Task ID: ${resultData}")
                } else {
                    rtn = ProxmoxApiUtil.validateApiResponse(results, "Failed to create image template ${imageName} (VMID ${nextId})")
                }
            } else if (!results.success) { // No content and not successful
                 rtn = ProxmoxApiUtil.validateApiResponse(results, "Failed to create image template ${imageName} (VMID ${nextId})")
            } else { // Success but no content, which is unusual for a create operation that should return a task ID.
                 rtn.msg = "Create image template response for ${imageName} (VMID ${nextId}) had no content but was marked success. Assuming failure as task ID is expected."
                 log.warn(rtn.msg)
                 // rtn.success remains false
            }
        } catch (e) {
            log.error("Exception creating image template ${imageName} (VMID ${nextId}) on node ${nodeId}: ${e.message}", e)
            // rtn.success is already false
            rtn.msg = "Exception creating image template ${imageName} on node ${nodeId}: ${e.message}"
        }
        return rtn
    }


    static ServiceResponse waitForCloneToComplete(HttpApiClient client, Map authConfig, String templateId, String vmId, String nodeId, Long timeoutInSec) {
        Long timeout = timeoutInSec * 1000
        Long duration = 0
        log.debug("waitForCloneToComplete: $templateId")

        try {
            def tokenCfg = getApiV2Token(authConfig).data
            def opts = [
                    headers: [
                            'Content-Type': 'application/json',
                            'Cookie': "PVEAuthCookie=$tokenCfg.token",
                            'CSRFPreventionToken': tokenCfg.csrfToken
                    ],
                    contentType: ContentType.APPLICATION_JSON,
                    ignoreSSL: ProxmoxSslUtil.IGNORE_SSL
            ]

            log.debug("Checking VM Status after clone template $templateId to VM $vmId on node $nodeId")
            log.debug("Path is: $authConfig.apiUrl${authConfig.v2basePath}/nodes/$nodeId/qemu/$vmId/config")

            while (duration < timeout) {
                log.info("Checking VM $vmId status on node $nodeId")
                def results = ProxmoxApiUtil.callJsonApiWithRetry(
                        client,
                        authConfig.apiUrl,
                        "${authConfig.v2basePath}/nodes/$nodeId/qemu/$vmId/config",
                        null,
                        null,
                        new HttpApiClient.RequestOptions(opts),
                        'GET'
                )

                if (!results.success) {
                    return ProxmoxApiUtil.validateApiResponse(results, "Error checking VM clone result status")
                }

                def resultData = new JsonSlurper().parseText(results.content)
                log.info("Check results: $resultData")
                if (!resultData.data.containsKey("lock")) {
                    return results
                } else {
                    log.info("VM Still Locked, wait ${API_CHECK_WAIT_INTERVAL}ms and check again...")
                }
                sleep(API_CHECK_WAIT_INTERVAL)
                duration += API_CHECK_WAIT_INTERVAL
            }
            return new ServiceResponse(success: false, msg: "Timeout", data: "Timeout")
        } catch(e) {
            log.error "Error Checking VM Clone Status: ${e}", e
            return ServiceResponse.error("Error Checking VM Clone Status: ${e}")
        }
    }


    static cloneTemplate(HttpApiClient client, Map authConfig, String templateId, String name, String nodeId, Long vcpus, Long ram) {
        log.debug("cloneTemplate: $templateId")

        def rtn = new ServiceResponse(success: true)
        def nextId = callListApiV2(client, "cluster/nextid", authConfig).data
        log.debug("Next VM Id is: $nextId")

        try {
            def tokenCfg = getApiV2Token(authConfig).data
            rtn.data = []
            def opts = [
                    headers: [
                            'Content-Type': 'application/json',
                            'Cookie': "PVEAuthCookie=$tokenCfg.token",
                            'CSRFPreventionToken': tokenCfg.csrfToken
                    ],
                    body: [
                            newid: nextId,
                            node: nodeId,
                            vmid: templateId,
                            name: name,
                            full: true
                    ],
                    contentType: ContentType.APPLICATION_JSON,
                    ignoreSSL: ProxmoxSslUtil.IGNORE_SSL
            ]

            log.debug("Cloning template $templateId to VM $name($nextId) on node $nodeId")
            log.debug("Path is: $authConfig.apiUrl${authConfig.v2basePath}/nodes/$nodeId/qemu/$templateId/clone")
            log.debug("Body data is: $opts.body")
            def results = ProxmoxApiUtil.callJsonApiWithRetry(
                    client,
                    authConfig.apiUrl,
                    "${authConfig.v2basePath}/nodes/$nodeId/qemu/$templateId/clone",
                    null,
                    null,
                    new HttpApiClient.RequestOptions(opts),
                    'POST'
            )

            def resultData = new JsonSlurper().parseText(results.content)

            if(results?.success && !results?.hasErrors()) {
                rtn.success = true
                rtn.data = resultData

                ServiceResponse cloneWaitResult = waitForCloneToComplete(new HttpApiClient(), authConfig, templateId, nextId, nodeId, 3600L)

                if (!cloneWaitResult?.success) {
                    return ServiceResponse.error("Error Provisioning VM. Wait for clone error: ${cloneWaitResult}")
                }

                log.info("Resizing newly cloned VM. Spec: CPU $vcpus, RAM $ram")
                ServiceResponse rtnResize = resizeVMCompute(new HttpApiClient(), authConfig, nodeId, nextId, vcpus, ram)

                if (!rtnResize?.success) {
                    return ServiceResponse.error("Error Sizing VM Compute. Resize compute error: ${rtnResize}")
                }

                rtn.data.vmId = nextId
            } else {
                rtn.msg = "Provisioning failed: ${results.toMap()}"
                rtn.success = false
            }
        } catch(e) {
            log.error "Error Provisioning VM: ${e}", e
            return ServiceResponse.error("Error Provisioning VM: ${e}")
        }
        return rtn
    }


    static ServiceResponse listProxmoxDatastores(HttpApiClient client, Map authConfig) {
        log.debug("listProxmoxDatastores...")

        var allowedDatastores = ["rbd", "cifs", "zfspool", "nfs", "lvmthin", "lvm"]

        ServiceResponse datastoreResults = callListApiV2(client, "storage", authConfig)
        List<Map> validDatastores = []
        String queryNode = ""
        String randomNode = null
        for (ds in datastoreResults.data) {
            if (allowedDatastores.contains(ds.type)) {
                if (ds.containsKey("nodes")) {
                    //some pools don't belong to any node, but api path needs node for status details
                    queryNode = ((String) ds.nodes).split(",")[0]
                } else {
                    if (!randomNode) {
                        randomNode = listProxmoxHypervisorHosts(client, authConfig).data.get(0).node
                    }
                    queryNode = randomNode
                }

                Map dsInfo = callListApiV2(client, "nodes/${queryNode}/storage/${ds.storage}/status", authConfig).data
                ds.total = dsInfo.total
                ds.avail = dsInfo.avail
                ds.used = dsInfo.used
                ds.enabled = dsInfo.enabled

                validDatastores += ds
            } else {
                log.warn("Storage ${ds} ignored...")
            }
        }
        datastoreResults.data = validDatastores
        return datastoreResults
    }


    static ServiceResponse listProxmoxNetworks(HttpApiClient client, Map authConfig) {
        log.debug("listProxmoxNetworks...")

        Collection<Map> networks = []
        Set<String> ifaces = []
        ServiceResponse hosts = listProxmoxHypervisorHosts(client, authConfig)

        hosts.data.each {
            ServiceResponse hostNetworks = callListApiV2(client, "nodes/${it.node}/network", authConfig)
            hostNetworks.data.each { Map network ->
                if (network?.type == 'bridge' && !ifaces.contains(network?.iface)) {
                    network.networkAddress = ""
                    if (network.containsKey("cidr") && network['cidr']) {
                        network.networkAddress = ProxmoxMiscUtil.getNetworkAddress(network.cidr)
                    }
                    networks << (network)
                    ifaces << network.iface
                }
            }
        }

        return new ServiceResponse(success: true, data: networks)
    }


    static ServiceResponse listTemplates(HttpApiClient client, Map authConfig) {
        log.debug("API Util listTemplates")
        def vms = []
        def qemuVMs = callListApiV2(client, "cluster/resources", authConfig)
        qemuVMs.data.each { Map vm ->
            if (vm?.template == 1 && vm?.type == "qemu") {
                vm.ip = "0.0.0.0"
                def vmCPUInfo = callListApiV2(client, "nodes/$vm.node/qemu/$vm.vmid/config", authConfig)
                vm.maxCores = (vmCPUInfo?.data?.data?.sockets?.toInteger() ?: 0) * (vmCPUInfo?.data?.data?.cores?.toInteger() ?: 0)
                vm.coresPerSocket = vmCPUInfo?.data?.data?.cores?.toInteger() ?: 0
                vms << vm
            }
        }
        return new ServiceResponse(success: true, data: vms)
    }


    static ServiceResponse listVMs(HttpApiClient client, Map authConfig) {
        log.debug("API Util listVMs - Enhanced")
        def vms = []
        // The 'client' parameter passed to listVMs is from VMSync, which is instantiated in ProxmoxVeCloudProvider.refresh.
        // That client in refresh() IS closed. So the client param here is managed by the caller.
        // The internalClient here is for calls within listVMs itself for per-VM details.
        HttpApiClient internalClient = new HttpApiClient()
        try {
            // The initial call to "cluster/resources" should use the client passed to listVMs (from VMSync).
            def qemuVMsResponse = callListApiV2(client, "cluster/resources", authConfig)
            if (!qemuVMsResponse.success) {
                log.error("Failed to list cluster resources: ${qemuVMsResponse.msg}")
                return qemuVMsResponse
            }

            qemuVMsResponse.data.each { Map vm ->
                if (vm?.template == 0 && vm?.type == "qemu" && vm.node != null && vm.vmid != null) {
                    log.debug("Enhanced listVMs - Processing VM: vmid=${vm.vmid}, node=${vm.node}, name=${vm.name ?: 'N/A'}") // More detailed log
                    // Initialize detailed structures
                    vm.disks = []
                    vm.networkInterfaces = []
                    vm.qemuAgent = [status: 'unknown'] // Default status

                    // 1. Get full VM config for disks and network interfaces
                    // These per-VM calls should use the internalClient
                    ServiceResponse vmConfigResponse = callListApiV2(internalClient, "nodes/${vm.node}/qemu/${vm.vmid}/config", authConfig)
                    if (vmConfigResponse.success && vmConfigResponse.data) {
                        Map configData = vmConfigResponse.data
                        vm.maxCores = (configData.sockets ?: 1).toInteger() * (configData.cores ?: 1).toInteger()
                        vm.coresPerSocket = (configData.cores ?: 1).toInteger()

                        // Parse Disks
                        configData.each { key, value ->
                            if (key ==~ /(scsi|ide|sata|virtio)\d+/) {
                                Map diskDetails = parseDiskEntry(key, value.toString())
                                if (diskDetails) vm.disks << diskDetails
                            }
                            // Parse Network Interfaces
                            if (key ==~ /net\d+/) {
                                Map nicDetails = parseNetworkInterfaceEntry(key, value.toString())
                                if (nicDetails) vm.networkInterfaces << nicDetails
                            }
                        }
                        // Consolidate memory from config
                        vm.maxmem = configData.memory ? (configData.memory.toLong() * 1024L * 1024L) : vm.maxmem // memory is in MB
                    } else {
                        log.warn("Could not retrieve config for VM ${vm.vmid} on node ${vm.node}: ${vmConfigResponse.msg}")
                        // Still attempt to get CPU info from main vm resource if config fails
                        if(vm.maxcpu) { // maxcpu from cluster/resources is vCPUs
                             vm.maxCores = vm.maxcpu.toInteger()
                        }
                         vm.coresPerSocket = 1 // Default if not found
                    }
                    
                    // 2. Get QEMU Agent Status and Info (including IPs from agent)
                    vm.ip = "0.0.0.0" // Default IP
                    // These per-VM calls should use the internalClient
                    ServiceResponse agentOsInfoResponse = callListApiV2(internalClient, "nodes/${vm.node}/qemu/${vm.vmid}/agent/get-osinfo", authConfig)
                    if (agentOsInfoResponse.success && agentOsInfoResponse.data?.result) {
                        vm.qemuAgent = [status: 'running', data: agentOsInfoResponse.data.result]
                        // Try to get IPs from agent if available - this is often more reliable
                        ServiceResponse agentNetworkInfo = callListApiV2(internalClient, "nodes/${vm.node}/qemu/${vm.vmid}/agent/network-get-interfaces", authConfig)
                        if (agentNetworkInfo.success && agentNetworkInfo.data?.result) {
                            def agentNics = agentNetworkInfo.data.result
                            vm.qemuAgent.networkInterfaces = agentNics // Store raw agent NIC data
                            def primaryIp = findPrimaryIpFromAgentNics(agentNics)
                            if (primaryIp) vm.ip = primaryIp
                        }
                    } else {
                        // Handle common error patterns for agent not running or not installed
                        String errorContent = agentOsInfoResponse.content?.toLowerCase() ?: ""
                        if (agentOsInfoResponse.errorCode == 500 && (errorContent.contains("qemu agent not running") || errorContent.contains("guest agent is not running"))) {
                             vm.qemuAgent = [status: 'not_running']
                        } else if (agentOsInfoResponse.errorCode == 500 && errorContent.contains("disabled")) {
                             vm.qemuAgent = [status: 'disabled']
                        } else {
                             vm.qemuAgent = [status: 'error', details: agentOsInfoResponse.msg ?: agentOsInfoResponse.content]
                        }
                        log.debug("QEMU agent for VM ${vm.vmid} not responsive or error: ${vm.qemuAgent.status} - ${vm.qemuAgent.details ?: ''}")
                        // Fallback: if agent failed, use the first IP from parsed netX interfaces if available
                        if (vm.ip == "0.0.0.0" && vm.networkInterfaces) {
                            vm.networkInterfaces.find { nic ->
                                // Proxmox config for netX does not directly contain IP. This requires agent.
                                // The initial cluster/resources call might have some IP, but it's unreliable.
                                // For now, without agent, IP remains 0.0.0.0 unless cluster/resources provided something better initially.
                                // This part of IP detection might need further refinement if agent is consistently unavailable.
                            }
                        }
                    }
                     // Ensure essential fields from cluster/resources are preserved if not overwritten by more detailed calls
                    vm.name = vm.name ?: "Unknown VM ${vm.vmid}"
                    vm.status = vm.status ?: "unknown" // 'running', 'stopped' from cluster/resources
                    vm.maxmem = vm.maxmem ?: 0L // From cluster/resources (bytes)
                    vm.maxdisk = vm.maxdisk ?: 0L // From cluster/resources (bytes) - this will be replaced by sum of actual disks

                    vms << vm
                }
            }
        } catch (Exception e) {
            log.error("Error in enhanced listVMs: ${e.message}", e)
            return ServiceResponse.error("Error fetching detailed VM list: ${e.message}")
        } finally {
            internalClient?.shutdownClient()
        }
        return new ServiceResponse(success: true, data: vms)
    }

    private static Map parseDiskEntry(String key, String value) {
        // Example: virtio0: local-lvm:vm-102-disk-0,size=32G,iothread=1
        // Example: scsi0: local:iso/proxmox-mailgateway_7.1-1.iso,media=cdrom,size=1020184K
        try {
            def parts = value.split(',')
            def storagePart = parts[0]
            def diskDetails = [name: key, type: key.replaceAll(/\d+$/, '')]

            def storageMatcher = (storagePart =~ /([^:]+):(.*)/)
            if (storageMatcher.find()) {
                diskDetails.storage = storageMatcher.group(1)
                diskDetails.file = storagePart // Full path like local-lvm:vm-102-disk-0 or local:iso/file.iso
            } else {
                diskDetails.storage = storagePart // Could be just storage name for unused drives or special cases
                diskDetails.file = storagePart
            }
            
            parts[1..-1].each { param ->
                def pair = param.split('=')
                if (pair.length == 2) {
                    switch (pair[0].toLowerCase()) {
                        case "size":
                            diskDetails.sizeRaw = pair[1]
                            diskDetails.sizeBytes = parseSizeToBytes(pair[1])
                            break
                        case "format":
                            diskDetails.format = pair[1]
                            break
                        case "media":
                            diskDetails.media = pair[1] // cdrom, disk
                            break
                        // Add other params like iothread, ssd, etc. if needed
                    }
                }
            }
            // Default format if not specified (e.g. for CDROM)
            if (!diskDetails.format && diskDetails.media == 'disk') diskDetails.format = 'raw' // Or 'qcow2' - Proxmox default might vary
            if (diskDetails.media == 'cdrom') diskDetails.isCdRom = true


            return diskDetails
        } catch (Exception e) {
            log.error("Failed to parse disk entry '${key}: ${value}': ${e.message}")
            return null
        }
    }

    private static Map parseNetworkInterfaceEntry(String key, String value) {
        // Example: net0: virtio=AA:BB:CC:DD:EE:FF,bridge=vmbr0,tag=100,firewall=1
        try {
            def nicDetails = [name: key]
            def parts = value.split(',')
            
            def modelMacPart = parts[0] // virtio=AA:BB:CC:DD:EE:FF or e1000=...
            def modelMacMatcher = (modelMacPart =~ /([^=]+)=([^,]+)/)
            if (modelMacMatcher.find()) {
                nicDetails.model = modelMacMatcher.group(1)
                nicDetails.macAddress = modelMacMatcher.group(2)
            } else {
                 // Fallback for configurations like 'net0: vmbr0' (though rare for QEMU, more for CTs or specific setups)
                nicDetails.model = modelMacPart // Or could be bridge name directly
            }

            parts[1..-1].each { param ->
                def pair = param.split('=')
                if (pair.length == 2) {
                    switch (pair[0].toLowerCase()) {
                        case "bridge":
                            nicDetails.bridge = pair[1]
                            break
                        case "tag":
                            nicDetails.vlanTag = pair[1]
                            break
                        case "firewall":
                            nicDetails.firewall = pair[1] == "1"
                            break
                        // rate, link_down, etc.
                    }
                } else if (nicDetails.model && !nicDetails.bridge && parts.length == 1) { 
                    // Handle case like net0: bridge_name (no model explicitly, PVE defaults it)
                    // This is less common with explicit model=MAC, but as a fallback.
                    nicDetails.bridge = pair[0]
                }
            }
            return nicDetails
        } catch (Exception e) {
            log.error("Failed to parse NIC entry '${key}: ${value}': ${e.message}")
            return null
        }
    }

    static long parseSizeToBytes(String sizeStr) {
        if (sizeStr == null || sizeStr.isEmpty()) return 0L
        def matcher = sizeStr =~ /(\d+)([KMGTP]?)/
        if (matcher.find()) {
            long size = matcher.group(1).toLong()
            String unit = matcher.group(2).toUpperCase()
            switch (unit) {
                case 'K': return size * 1024L
                case 'M': return size * 1024L * 1024L
                case 'G': return size * 1024L * 1024L * 1024L
                case 'T': return size * 1024L * 1024L * 1024L * 1024L
                case 'P': return size * 1024L * 1024L * 1024L * 1024L * 1024L
                default: return size // Assuming bytes if no unit or unknown unit
            }
        }
        return 0L
    }
    
    private static String findPrimaryIpFromAgentNics(List agentNics) {
        if (!agentNics) return null
        String firstIp = null
        for (nic in agentNics) {
            if (nic.'ip-addresses') {
                for (ipInfo in nic.'ip-addresses') {
                    if (ipInfo.'ip-address-type' == 'ipv4' && ipInfo.'ip-address' != '127.0.0.1') {
                        if (firstIp == null) firstIp = ipInfo.'ip-address'
                        // Add logic here if a "primary" marker exists or specific interface name is preferred
                        // For now, returning the first valid non-localhost IPv4
                        return ipInfo.'ip-address'
                    }
                }
            }
        }
        return firstIp // Returns the first IP found, or null if none
    }


    static ServiceResponse listProxmoxHypervisorHosts(HttpApiClient client, Map authConfig) {
        log.debug("listProxmoxHosts...")

        def nodes = callListApiV2(client, "nodes", authConfig).data
        nodes.each {
            def nodeNetworkInfo = callListApiV2(client, "nodes/$it.node/network", authConfig)
            def ipAddress = nodeNetworkInfo.data[0].address ?: nodeNetworkInfo.data[1].address
            it.ipAddress = ipAddress
        }

        return new ServiceResponse(success: true, data: nodes)
    }
    
    
    private static ServiceResponse callListApiV2(HttpApiClient client, String path, Map authConfig) {
        log.debug("callListApiV2 for path: ${path}")
        def rtn = new ServiceResponse(success: false) 
        try {
            ServiceResponse tokenCfgResponse = getApiV2Token(authConfig)
            if (!tokenCfgResponse.success) {
                // Propagate the detailed error from getApiV2Token directly
                return tokenCfgResponse 
            }
            def tokenCfg = tokenCfgResponse.data

            // rtn.data can be initialized based on expected type, but Proxmox list APIs usually return an array.
            // Defaulting to null and setting on success is also fine.
            // For consistency, let's initialize to an empty list if we expect a list.
            rtn.data = [] 

            def opts = new HttpApiClient.RequestOptions(
                    headers: [
                        'Content-Type': 'application/json',
                        'Cookie': "PVEAuthCookie=${tokenCfg.token}",
                        'CSRFPreventionToken': tokenCfg.csrfToken
                    ],
                    contentType: ContentType.APPLICATION_JSON,
                    ignoreSSL: ProxmoxSslUtil.IGNORE_SSL
            )
            def results = ProxmoxApiUtil.callJsonApiWithRetry(client, authConfig.apiUrl, "${authConfig.v2basePath}/${path}", null, null, opts, 'GET')
            
            // Proxmox specific: successful list operations usually have results.data.data as a list.
            if(results?.success && results.data?.data != null) { 
                rtn.success = true
                rtn.data = results.data.data 
                log.debug("callListApiV2 successful for path ${path}. Number of records: ${rtn.data instanceof Collection ? rtn.data.size() : (rtn.data instanceof Map ? 1 : 'N/A')}")
            } else if (results?.success && results.data?.data == null) {
                // This case handles when Proxmox returns success (HTTP 200) but the "data" array is missing or null (e.g. empty list might be `{"data":[]}` or just `{"data":null}`)
                // If results.data itself is the payload (e.g. a single object GET not in 'data' wrapper, or an empty list directly as results.data)
                // This part of the logic is more for specific GETs of single objects if they were to use callListApiV2.
                // For list APIs, Proxmox is fairly consistent with `{"data": [...]}`. So `results.data.data == null` on success might mean empty list.
                // If `results.data` is `null` entirely, it's an issue. If `results.data` is an empty map/list, that's valid.
                if (results.data == null) { // Success but the entire 'data' object from HttpApiClient is null.
                    rtn.success = false // This is unexpected for a successful API call that should return data.
                    rtn.msg = "API call to ${path} succeeded but returned no data object."
                    log.warn(rtn.msg + " Raw content: ${results.content}")
                } else { // results.data is not null, but results.data.data is. This means an empty list or non-standard response.
                    rtn.success = true // Treat as success with empty list or the direct data.
                    rtn.data = results.data // Could be an empty list `[]` or a map for a single item.
                    log.warn("callListApiV2 for path ${path}: 'data.data' was null or not present. Using 'results.data' as payload. This might be an empty list or a single item. Payload: ${results.data}")
                }
            } else { // results.success is false
                rtn = ProxmoxApiUtil.validateApiResponse(results, "API call to ${path} failed")
            }
        } catch(e) {
            log.error("Exception in callListApiV2 for path ${path}: ${e.message}", e)
            rtn.success = false // Ensure success is false on exception
            rtn.msg = "Exception during API call to ${path}: ${e.message}"
        }
        return rtn
    }


    private static ServiceResponse getApiV2Token(Map authConfig) {
        def path = "access/ticket"
        log.debug("getApiV2Token: path: ${path} for apiUrl: ${authConfig.apiUrl}") // Added apiUrl for context
        HttpApiClient client = new HttpApiClient() 
        def rtn = new ServiceResponse(success: false)
        try {
            if (!authConfig.username || !authConfig.password) {
                rtn.msg = "Username or password missing in authConfig for getApiV2Token."
                log.error(rtn.msg)
                return rtn // Early exit if essential auth info is missing
            }
            def encUid = URLEncoder.encode((String) authConfig.username, "UTF-8")
            def encPwd = URLEncoder.encode((String) authConfig.password, "UTF-8")
            def bodyStr = "username=" + "$encUid" + "&password=$encPwd"

            HttpApiClient.RequestOptions opts = new HttpApiClient.RequestOptions(
                    headers: ['Content-Type':'application/x-www-form-urlencoded'],
                    body: bodyStr,
                    contentType: ContentType.APPLICATION_FORM_URLENCODED,
                    ignoreSSL: ProxmoxSslUtil.IGNORE_SSL
            )
            def results = ProxmoxApiUtil.callJsonApiWithRetry(client, authConfig.apiUrl,"${authConfig.v2basePath}/${path}", null, null, opts, 'POST')

            // Log the raw response content for debugging, especially for auth issues
            log.debug("getApiV2Token API response raw content: ${results.content}")
            log.debug("getApiV2Token API response success: ${results.success}, errors: ${results.errors}, msg: ${results.msg}, errorCode: ${results.errorCode}")


            if(results?.success && results.data?.data) { // Check results.data.data for actual token info
                rtn.success = true
                def tokenData = results.data.data
                rtn.data = [csrfToken: tokenData.CSRFPreventionToken, token: tokenData.ticket]
            } else {
                rtn.success = false
                rtn.msg = results.msg ?: results.content ?: "Failed to retrieve Proxmox API token. ErrorCode: ${results.errorCode ?: 'N/A'}"
                log.error("Error retrieving Proxmox API token for ${authConfig.username}@${authConfig.apiUrl}: ${rtn.msg}")
            }
        } catch(e) {
            log.error "Exception in getApiV2Token for ${authConfig.username}@${authConfig.apiUrl}: ${e.message}", e
            rtn.success = false
            rtn.msg = "Exception retrieving Proxmox API token: ${e.message}"
        } finally {
            // Ensure the locally created client is shut down
            client?.shutdownClient()
        }
        return rtn
    }

/*    private static ServiceResponse getApiV2Token(String uid, String pwd, String baseUrl) {
        def path = "access/ticket"
        log.debug("getApiV2Token: path: ${path}")
        HttpApiClient client = new HttpApiClient()

        def rtn = new ServiceResponse(success: false)
        try {

            def encUid = URLEncoder.encode((String) uid, "UTF-8")
            def encPwd = URLEncoder.encode((String) pwd, "UTF-8")
            def bodyStr = "username=" + "$encUid" + "&password=$encPwd"

            HttpApiClient.RequestOptions opts = new HttpApiClient.RequestOptions(
                    headers: ['Content-Type':'application/x-www-form-urlencoded'],
                    body: bodyStr,
                    contentType: ContentType.APPLICATION_FORM_URLENCODED,
                    ignoreSSL: ProxmoxSslUtil.IGNORE_SSL
            )
            def results = ProxmoxApiUtil.callJsonApiWithRetry(client, baseUrl,"${API_BASE_PATH}/${path}", opts, 'POST')

            log.debug("getApiV2Token API request results: ${results.toMap()}")
            if(results?.success && !results?.hasErrors()) {
                rtn.success = true
                def tokenData = results.data.data
                rtn.data = [csrfToken: tokenData.CSRFPreventionToken, token: tokenData.ticket]

            } else {
                rtn.success = false
                rtn.msg = "Error retrieving token: $results.data"
                log.error("Error retrieving token: $results.data")
            }
            return rtn
        } catch(e) {
            log.error "Error in getApiV2Token: ${e}", e
            rtn.msg = "Error in getApiV2Token: ${e}"
            rtn.success = false
        }
        return rtn
    }
    */

    static ServiceResponse addVMDisk(HttpApiClient client, Map authConfig, String nodeName, String vmId, String storageName, Integer diskSizeGB, String diskType) {
        log.debug("addVMDisk: vmId=${vmId}, nodeName=${nodeName}, storageName=${storageName}, diskSizeGB=${diskSizeGB}, diskType=${diskType}")

        HttpApiClient internalClient = new HttpApiClient() // For fetching VM config without interfering with the passed client
        try {
            // 1. Authentication for internal calls if needed, or reuse main token.
            // getApiV2Token creates its own client.
            def tokenCfgResponse = getApiV2Token(authConfig)
            if(!tokenCfgResponse.success) {
                return ServiceResponse.error("Failed to get API token for Proxmox in addVMDisk: ${tokenCfgResponse.msg}")
            }
            def tokenCfg = tokenCfgResponse.data
            // Note: The 'client' parameter passed to addVMDisk is for the final POST.
            // The GET for vmConfig uses 'internalClient'.

            // 2. Get current VM config to find next available disk index
            def vmConfigOpts = new HttpApiClient.RequestOptions(
                headers: [
                    'Cookie': "PVEAuthCookie=${tokenCfg.token}",
                    'CSRFPreventionToken': tokenCfg.csrfToken
                ],
                contentType: ContentType.APPLICATION_JSON,
                ignoreSSL: ProxmoxSslUtil.IGNORE_SSL
            )
            String configPath = "${authConfig.v2basePath}/nodes/${nodeName}/qemu/${vmId}/config"
            log.debug("Getting VM config from: ${authConfig.apiUrl}${configPath}")
            def vmConfigResponse = ProxmoxApiUtil.callJsonApiWithRetry(
                internalClient,
                authConfig.apiUrl,
                configPath,
                null,
                null,
                vmConfigOpts,
                'GET'
            )

            if (!vmConfigResponse.success || !vmConfigResponse.data?.data) {
                log.error("Failed to get VM config for ${vmId}: ${vmConfigResponse.msg} - ${vmConfigResponse.content}")
                return ServiceResponse.error("Failed to get VM config for ${vmId}: ${vmConfigResponse.msg ?: 'No data returned'}")
            }

            Map currentConfig = vmConfigResponse.data.data
            log.debug("Current VM config: ${currentConfig}")

            // 3. Determine next available disk index
            int maxDisks
            switch (diskType.toLowerCase()) {
                case "scsi": maxDisks = 30; break // scsi0-scsi30
                case "virtio": maxDisks = 15; break // virtio0-virtio15
                case "ide": maxDisks = 3; break // ide0-ide3
                case "sata": maxDisks = 5; break // sata0-sata5
                default:
                    return ServiceResponse.error("Unsupported diskType: ${diskType}. Must be one of scsi, virtio, ide, sata.")
            }

            String diskPrefix = diskType.toLowerCase()
            int nextDiskIndex = -1
            for (int i = 0; i <= maxDisks; i++) {
                if (!currentConfig.containsKey("${diskPrefix}${i}")) {
                    nextDiskIndex = i
                    break
                }
            }

            if (nextDiskIndex == -1) {
                return ServiceResponse.error("No available disk slot found for type ${diskType} on VM ${vmId}.")
            }

            String newDiskParamName = "${diskPrefix}${nextDiskIndex}"
            String newDiskParamValue = "${storageName},size=${diskSizeGB}G"
            log.info("Determined next available disk: ${newDiskParamName} with value: ${newDiskParamValue}")

            // 4. Execute Add Disk API Call (POST to config)
            def addDiskBody = [ (newDiskParamName) : newDiskParamValue ]
            def addDiskOpts = new HttpApiClient.RequestOptions(
                headers: [
                    'Content-Type': 'application/json',
                    'Cookie': "PVEAuthCookie=${tokenCfg.token}",
                    'CSRFPreventionToken': tokenCfg.csrfToken
                ],
                body: addDiskBody,
                contentType: ContentType.APPLICATION_JSON,
                ignoreSSL: ProxmoxSslUtil.IGNORE_SSL
            )

            log.debug("Adding disk with POST to: ${authConfig.apiUrl}${configPath}")
            log.debug("POST body: ${addDiskBody}")

            def addDiskResult = ProxmoxApiUtil.callJsonApiWithRetry(
                client,
                authConfig.apiUrl,
                configPath,
                null,
                null,
                addDiskOpts,
                'POST'
            )

            log.debug("Add disk API response: ${addDiskResult.toMap()}")

            if (addDiskResult.success) {
                // Proxmox API for config update returns a task ID.
                // We might want to check the task status in a more advanced implementation.
                // For now, a successful API call is considered sufficient.
                log.info("Successfully initiated add disk operation for ${vmId}. Disk: ${newDiskParamName}, Task ID: ${addDiskResult.data?.data}")
                return ServiceResponse.success("Disk ${newDiskParamName} added successfully. Task ID: ${addDiskResult.data?.data}", [taskId: addDiskResult.data?.data])
            } else {
                return ProxmoxApiUtil.validateApiResponse(addDiskResult, "Failed to add disk ${newDiskParamName} to VM ${vmId}")
            }

        } catch (e) {
            log.error("Error adding disk to VM ${vmId}: ${e.message}", e)
            return ServiceResponse.error("Error adding disk to VM ${vmId}: ${e.message}")
        } finally {
            internalClient?.shutdownClient()
        }
    }

    static ServiceResponse addVMNetworkInterface(HttpApiClient client, Map authConfig, String nodeName, String vmId, String bridgeName, String model, String vlanTag, Boolean firewallEnabled) {
        log.debug("addVMNetworkInterface: vmId=${vmId}, nodeName=${nodeName}, bridgeName=${bridgeName}, model=${model}, vlanTag=${vlanTag}, firewallEnabled=${firewallEnabled}")

        HttpApiClient internalClient = new HttpApiClient() // For fetching VM config
        try {
            // 1. Authentication for internal calls if needed
            def tokenCfgResponse = getApiV2Token(authConfig) // Ensures token is fresh for subsequent calls if needed by internalClient
            if(!tokenCfgResponse.success) {
                 return ServiceResponse.error("Failed to get API token for Proxmox in addVMNetworkInterface: ${tokenCfgResponse.msg}")
            }
            def tokenCfg = tokenCfgResponse.data
            // Note: The 'client' parameter passed to addVMNetworkInterface is for the final POST.
            // The GET for vmConfig uses 'internalClient'.

            // 2. Get current VM config to find next available network interface index
            def vmConfigOpts = new HttpApiClient.RequestOptions(
                headers: [
                    'Cookie': "PVEAuthCookie=${tokenCfg.token}",
                    'CSRFPreventionToken': tokenCfg.csrfToken
                ],
                contentType: ContentType.APPLICATION_JSON,
                ignoreSSL: ProxmoxSslUtil.IGNORE_SSL
            )
            String configPath = "${authConfig.v2basePath}/nodes/${nodeName}/qemu/${vmId}/config"
            log.debug("Getting VM config from: ${authConfig.apiUrl}${configPath}")
            def vmConfigResponse = ProxmoxApiUtil.callJsonApiWithRetry(
                internalClient,
                authConfig.apiUrl,
                configPath,
                null,
                null,
                vmConfigOpts,
                'GET'
            )

            if (!vmConfigResponse.success || !vmConfigResponse.data?.data) {
                log.error("Failed to get VM config for ${vmId}: ${vmConfigResponse.msg} - ${vmConfigResponse.content}")
                return ServiceResponse.error("Failed to get VM config for ${vmId}: ${vmConfigResponse.msg ?: 'No data returned'}")
            }

            Map currentConfig = vmConfigResponse.data.data
            log.debug("Current VM config: ${currentConfig}")

            // 3. Determine next available network interface index (net0, net1, ...)
            int maxNics = 31 // Proxmox typically supports net0 through net31
            int nextNicIndex = -1
            for (int i = 0; i <= maxNics; i++) {
                if (!currentConfig.containsKey("net${i}")) {
                    nextNicIndex = i
                    break
                }
            }

            if (nextNicIndex == -1) {
                return ServiceResponse.error("No available network interface slot found for VM ${vmId}.")
            }

            String newNicParamName = "net${nextNicIndex}"

            // 4. Construct Network Configuration String
            StringBuilder nicConfig = new StringBuilder()
            nicConfig.append("${bridgeName},model=${model}")
            if (vlanTag != null && !vlanTag.trim().isEmpty()) {
                nicConfig.append(",tag=${vlanTag.trim()}")
            }
            if (firewallEnabled != null && firewallEnabled) {
                nicConfig.append(",firewall=1")
            }
            String newNicParamValue = nicConfig.toString()
            log.info("Determined next available NIC: ${newNicParamName} with value: ${newNicParamValue}")

            // 5. Execute Add Network Interface API Call (POST to config)
            def addNicBody = [ (newNicParamName) : newNicParamValue ]
            def addNicOpts = new HttpApiClient.RequestOptions(
                headers: [
                    'Content-Type': 'application/json',
                    'Cookie': "PVEAuthCookie=${tokenCfg.token}",
                    'CSRFPreventionToken': tokenCfg.csrfToken
                ],
                body: addNicBody,
                contentType: ContentType.APPLICATION_JSON,
                ignoreSSL: ProxmoxSslUtil.IGNORE_SSL
            )

            log.debug("Adding NIC with POST to: ${authConfig.apiUrl}${configPath}")
            log.debug("POST body: ${addNicBody}")

            def addNicResult = ProxmoxApiUtil.callJsonApiWithRetry(
                client,
                authConfig.apiUrl,
                configPath,
                null,
                null,
                addNicOpts,
                'POST'
            )

            log.debug("Add NIC API response: ${addNicResult.toMap()}")

            if (addNicResult.success) {
                log.info("Successfully initiated add NIC operation for ${vmId}. NIC: ${newNicParamName}, Task ID: ${addNicResult.data?.data}")
                return ServiceResponse.success("Network interface ${newNicParamName} added successfully. Task ID: ${addNicResult.data?.data}", [taskId: addNicResult.data?.data])
            } else {
                return ProxmoxApiUtil.validateApiResponse(addNicResult, "Failed to add NIC ${newNicParamName} to VM ${vmId}")
            }

        } catch (e) {
            log.error("Error adding NIC to VM ${vmId}: ${e.message}", e)
            return ServiceResponse.error("Error adding NIC to VM ${vmId}: ${e.message}")
        } finally {
            internalClient?.shutdownClient()
        }
    }

    static ServiceResponse requestVMConsole(HttpApiClient client, Map authConfig, String nodeName, String vmId, String consoleType) {
        log.debug("requestVMConsole: vmId=${vmId}, nodeName=${nodeName}, consoleType=${consoleType}")

        try {
            // 1. Authentication for internal calls if needed
            def tokenCfgResponse = getApiV2Token(authConfig)  // Ensures token is fresh for subsequent calls
             if(!tokenCfgResponse.success) {
                 return ServiceResponse.error("Failed to get API token for Proxmox in requestVMConsole: ${tokenCfgResponse.msg}")
            }
            def tokenCfg = tokenCfgResponse.data
            // Note: The 'client' parameter passed to requestVMConsole is for the actual console request POST.

            // 2. Determine API path and parameters based on consoleType
            String endpointPathSegment
            switch (consoleType.toLowerCase()) {
                case "vnc":
                    endpointPathSegment = "vncproxy"
                    break
                // case "spice": // Example for future SPICE support
                //     endpointPathSegment = "spiceproxy"
                //     break
                // case "xtermjs": // Example for future xtermjs support
                // endpointPathSegment = "termproxy" // or similar, check Proxmox docs
                // break
                default:
                    return ServiceResponse.error("Unsupported console type: ${consoleType}. Supported types: 'vnc'.")
            }

            String apiPath = "${authConfig.v2basePath}/nodes/${nodeName}/qemu/${vmId}/${endpointPathSegment}"
            
            // Proxmox vncproxy is a POST request, potentially with an empty body or specific params if needed.
            // For vncproxy, the documentation suggests an empty POST or one with 'websocket': 1 for websocket-only.
            // Let's assume an empty body for now, which usually returns all necessary details.
            def requestBody = [:] // Empty body, or add specific params like ['websocket': 1] if needed

            def consoleOpts = new HttpApiClient.RequestOptions(
                headers: [
                    'Content-Type': 'application/json', // Proxmox API generally expects JSON for POST bodies
                    'Cookie': "PVEAuthCookie=${tokenCfg.token}",
                    'CSRFPreventionToken': tokenCfg.csrfToken
                ],
                body: requestBody,
                contentType: ContentType.APPLICATION_JSON,
                ignoreSSL: ProxmoxSslUtil.IGNORE_SSL
            )

            log.debug("Requesting ${consoleType} console with POST to: ${authConfig.apiUrl}${apiPath}")
            log.debug("POST body: ${requestBody}")

            def consoleResult = ProxmoxApiUtil.callJsonApiWithRetry(
                client,
                authConfig.apiUrl,
                apiPath,
                null,
                null,
                consoleOpts,
                'POST'
            )

            log.debug("${consoleType} console API response: ${consoleResult.toMap()}")

            if (consoleResult.success && consoleResult.data?.data) {
                Map consoleData = consoleResult.data.data
                // Add the console type to the returned data for clarity upstream
                consoleData.type = consoleType.toLowerCase()
                // The 'host' field from Proxmox might be '::1' if on the node itself.
                // The actual Proxmox server address (from authConfig.apiUrl) is usually needed.
                consoleData.proxmoxHost = new URL(authConfig.apiUrl).getHost()
                consoleData.proxmoxPort = new URL(authConfig.apiUrl).getPort() != -1 ? new URL(authConfig.apiUrl).getPort() : 8006 // Default Proxmox web port

                log.info("Successfully requested ${consoleType} console for VM ${vmId}. Details: ${consoleData}")
                return ServiceResponse.success("Console information retrieved successfully.", consoleData)
            } else {
                return ProxmoxApiUtil.validateApiResponse(consoleResult, "Failed to request ${consoleType} console for VM ${vmId}")
            }

        } catch (e) {
            log.error("Error requesting ${consoleType} console for VM ${vmId}: ${e.message}", e)
            return ServiceResponse.error("Error requesting ${consoleType} console for VM ${vmId}: ${e.message}")
        }
        // No client.shutdownClient() here as the client is passed in and managed by the caller
    }


    static ServiceResponse listSnapshots(HttpApiClient client, Map authConfig, String nodeId, String vmId) {
        log.debug("listSnapshots: vmId=${vmId}, nodeId=${nodeId}")
        try {
            String path = "nodes/${nodeId}/qemu/${vmId}/snapshot"
            ServiceResponse response = callListApiV2(client, path, authConfig)
            if (response.success) {
                log.info("Successfully listed snapshots for VM ${vmId} on node ${nodeId}.")
            } else {
                log.error("Failed to list snapshots for VM ${vmId} on node ${nodeId}: ${response.msg} - ${response.content}")
            }
            return response
        } catch (e) {
            log.error("Error listing snapshots for VM ${vmId}: ${e.message}", e)
            return ServiceResponse.error("Error listing snapshots for VM ${vmId}: ${e.message}")
        }
    }

    static ServiceResponse createSnapshot(HttpApiClient client, Map authConfig, String nodeId, String vmId, String snapshotName, String description) {
        log.debug("createSnapshot: vmId=${vmId}, nodeId=${nodeId}, snapshotName=${snapshotName}")
        try {
            def tokenCfgResponse = getApiV2Token(authConfig)
            if (!tokenCfgResponse.success) {
                return ServiceResponse.error("Failed to get API token for Proxmox in createSnapshot: ${tokenCfgResponse.msg}")
            }
            def tokenCfg = tokenCfgResponse.data

            String path = "${authConfig.v2basePath}/nodes/${nodeId}/qemu/${vmId}/snapshot"
            def requestBody = [
                snapname: snapshotName,
                description: description
            ]

            def opts = new HttpApiClient.RequestOptions(
                headers: [
                    'Content-Type': 'application/json',
                    'Cookie': "PVEAuthCookie=${tokenCfg.token}",
                    'CSRFPreventionToken': tokenCfg.csrfToken
                ],
                body: requestBody,
                contentType: ContentType.APPLICATION_JSON,
                ignoreSSL: ProxmoxSslUtil.IGNORE_SSL
            )

            log.debug("Creating snapshot with POST to: ${authConfig.apiUrl}${path}")
            log.debug("POST body: ${requestBody}")

            def results = ProxmoxApiUtil.callJsonApiWithRetry(client, 
                authConfig.apiUrl,
                path,
                null,
                null,
                opts,
                'POST'
            )

            log.debug("Create snapshot API response: ${results.toMap()}")

            if (results.success) {
                log.info("Successfully created snapshot ${snapshotName} for VM ${vmId}. Task ID: ${results.data?.data}")
                return ServiceResponse.success("Snapshot ${snapshotName} created successfully. Task ID: ${results.data?.data}", [taskId: results.data?.data])
            } else {
                log.error("Failed to create snapshot ${snapshotName} for VM ${vmId}: ${results.msg} - ${results.content}")
                return ServiceResponse.error("Failed to create snapshot ${snapshotName}: ${results.msg ?: results.content}")
            }
        } catch (e) {
            log.error("Error creating snapshot for VM ${vmId}: ${e.message}", e)
            return ServiceResponse.error("Error creating snapshot for VM ${vmId}: ${e.message}")
        }
    }

    static ServiceResponse deleteSnapshot(HttpApiClient client, Map authConfig, String nodeId, String vmId, String snapshotName) {
        log.debug("deleteSnapshot: vmId=${vmId}, nodeId=${nodeId}, snapshotName=${snapshotName}")
        try {
            def tokenCfgResponse = getApiV2Token(authConfig)
            if (!tokenCfgResponse.success) {
                return ServiceResponse.error("Failed to get API token for Proxmox in deleteSnapshot: ${tokenCfgResponse.msg}")
            }
            def tokenCfg = tokenCfgResponse.data

            String path = "${authConfig.v2basePath}/nodes/${nodeId}/qemu/${vmId}/snapshot/${snapshotName}"

            def opts = new HttpApiClient.RequestOptions(
                headers: [
                    'Content-Type': 'application/json',
                    'Cookie': "PVEAuthCookie=${tokenCfg.token}",
                    'CSRFPreventionToken': tokenCfg.csrfToken
                ],
                ignoreSSL: ProxmoxSslUtil.IGNORE_SSL
            )

            log.debug("Deleting snapshot with DELETE to: ${authConfig.apiUrl}${path}")

            def results = ProxmoxApiUtil.callJsonApiWithRetry(client, 
                authConfig.apiUrl,
                path,
                null,
                null,
                opts,
                'DELETE'
            )

            log.debug("Delete snapshot API response: ${results.toMap()}")

            if (results.success) {
                log.info("Successfully deleted snapshot ${snapshotName} for VM ${vmId}. Task ID: ${results.data?.data}")
                return ServiceResponse.success("Snapshot ${snapshotName} deleted successfully. Task ID: ${results.data?.data}", [taskId: results.data?.data])
            } else {
                log.error("Failed to delete snapshot ${snapshotName} for VM ${vmId}: ${results.msg} - ${results.content}")
                return ServiceResponse.error("Failed to delete snapshot ${snapshotName}: ${results.msg ?: results.content}")
            }
        } catch (e) {
            log.error("Error deleting snapshot for VM ${vmId}: ${e.message}", e)
            return ServiceResponse.error("Error deleting snapshot for VM ${vmId}: ${e.message}")
        }
    }

    static ServiceResponse rollbackSnapshot(HttpApiClient client, Map authConfig, String nodeId, String vmId, String snapshotName) {
        log.debug("rollbackSnapshot: vmId=${vmId}, nodeId=${nodeId}, snapshotName=${snapshotName}")
        try {
            def tokenCfgResponse = getApiV2Token(authConfig)
            if (!tokenCfgResponse.success) {
                return ServiceResponse.error("Failed to get API token for Proxmox in rollbackSnapshot: ${tokenCfgResponse.msg}")
            }
            def tokenCfg = tokenCfgResponse.data

            String path = "${authConfig.v2basePath}/nodes/${nodeId}/qemu/${vmId}/snapshot/${snapshotName}/rollback"
            
            def opts = new HttpApiClient.RequestOptions(
                headers: [
                    'Content-Type': 'application/json',
                    'Cookie': "PVEAuthCookie=${tokenCfg.token}",
                    'CSRFPreventionToken': tokenCfg.csrfToken
                ],
                body: [:], // Empty body for rollback
                contentType: ContentType.APPLICATION_JSON,
                ignoreSSL: ProxmoxSslUtil.IGNORE_SSL
            )

            log.debug("Rolling back snapshot with POST to: ${authConfig.apiUrl}${path}")

            def results = ProxmoxApiUtil.callJsonApiWithRetry(client, 
                authConfig.apiUrl,
                path,
                null,
                null,
                opts,
                'POST'
            )

            log.debug("Rollback snapshot API response: ${results.toMap()}")

            if (results.success) {
                log.info("Successfully rolled back to snapshot ${snapshotName} for VM ${vmId}. Task ID: ${results.data?.data}")
                return ServiceResponse.success("Snapshot ${snapshotName} rolled back successfully. Task ID: ${results.data?.data}", [taskId: results.data?.data])
            } else {
                log.error("Failed to roll back to snapshot ${snapshotName} for VM ${vmId}: ${results.msg} - ${results.content}")
                return ServiceResponse.error("Failed to roll back to snapshot ${snapshotName}: ${results.msg ?: results.content}")
            }
        } catch (e) {
            log.error("Error rolling back snapshot for VM ${vmId}: ${e.message}", e)
            return ServiceResponse.error("Error rolling back snapshot for VM ${vmId}: ${e.message}")
        }
    }

    static ServiceResponse removeVMDisk(HttpApiClient client, Map authConfig, String nodeName, String vmId, String diskName) {
        log.debug("removeVMDisk: vmId=${vmId}, nodeName=${nodeName}, diskName=${diskName}")

        try {
            // 1. Authentication
            def tokenCfgResponse = getApiV2Token(authConfig)
            if (!tokenCfgResponse.success) {
                return ServiceResponse.error("Failed to get API token for Proxmox in removeVMDisk: ${tokenCfgResponse.msg}")
            }
            def tokenCfg = tokenCfgResponse.data

            // 2. Construct API Path
            String configPath = "${authConfig.v2basePath}/nodes/${nodeName}/qemu/${vmId}/config"

            // 3. Create Request Body
            // Based on `qm set <vmid> --delete <key>`, the body should specify the disk to delete.
            // The key is the disk name, e.g., "scsi0", "virtio1".
            def requestBody = [
                delete: diskName
            ]

            // 4. Prepare RequestOptions
            def opts = new HttpApiClient.RequestOptions(
                headers: [
                    'Content-Type'       : 'application/json',
                    'Cookie'             : "PVEAuthCookie=${tokenCfg.token}",
                    'CSRFPreventionToken': tokenCfg.csrfToken
                ],
                body: requestBody,
                contentType: ContentType.APPLICATION_JSON,
                ignoreSSL: ProxmoxSslUtil.IGNORE_SSL
            )

            log.debug("Removing disk '${diskName}' from VM ${vmId} on node ${nodeName} with POST to: ${authConfig.apiUrl}${configPath}")
            log.debug("POST body: ${requestBody}")

            // 5. Make the API Call
            def results = ProxmoxApiUtil.callJsonApiWithRetry(client, 
                authConfig.apiUrl,
                configPath,
                null, // queryParams
                null, // body (using RequestOptions.body instead for POST)
                opts,
                'POST' // Using POST as per Proxmox API conventions for config changes
            )

            log.debug("Remove disk API response: ${results.toMap()}")

            // 6. Handle Response
            if (results.success) {
                // Proxmox API for config update usually returns a task ID.
                // A successful call (e.g., 200 OK) indicates the operation was accepted.
                log.info("Successfully initiated remove disk operation for VM ${vmId}, disk ${diskName}. Task ID: ${results.data?.data}")
                return ServiceResponse.success("Disk ${diskName} removal initiated successfully. Task ID: ${results.data?.data}", [taskId: results.data?.data])
            } else {
                // Handle cases like disk not found, or other API errors.
                // Proxmox might return a specific error message or code.
                String errorMessage = results.msg ?: results.content ?: "Unknown error"
                log.error("Failed to remove disk ${diskName} from VM ${vmId}: ${errorMessage}")
                // It might be useful to check results.errorCode or specific content if Proxmox has distinct errors for "disk not found"
                return ServiceResponse.error("Failed to remove disk ${diskName}: ${errorMessage}")
            }

        } catch (e) {
            log.error("Error removing disk ${diskName} from VM ${vmId}: ${e.message}", e)
            return ServiceResponse.error("Error removing disk ${diskName} from VM ${vmId}: ${e.message}")
        }
    }

    static ServiceResponse removeVMNetworkInterface(HttpApiClient client, Map authConfig, String nodeName, String vmId, String interfaceName) {
        log.debug("removeVMNetworkInterface: vmId=${vmId}, nodeName=${nodeName}, interfaceName=${interfaceName}")

        try {
            // 1. Authentication
            def tokenCfgResponse = getApiV2Token(authConfig)
            if (!tokenCfgResponse.success) {
                return ServiceResponse.error("Failed to get API token for Proxmox in removeVMNetworkInterface: ${tokenCfgResponse.msg}")
            }
            def tokenCfg = tokenCfgResponse.data

            // 2. Construct API Path
            String configPath = "${authConfig.v2basePath}/nodes/${nodeName}/qemu/${vmId}/config"

            // 3. Create Request Body
            // To remove an interface, we pass its name (e.g., net0) to the 'delete' parameter.
            def requestBody = [
                delete: interfaceName
            ]

            // 4. Prepare RequestOptions
            def opts = new HttpApiClient.RequestOptions(
                headers: [
                    'Content-Type'       : 'application/json',
                    'Cookie'             : "PVEAuthCookie=${tokenCfg.token}",
                    'CSRFPreventionToken': tokenCfg.csrfToken
                ],
                body: requestBody,
                contentType: ContentType.APPLICATION_JSON,
                ignoreSSL: ProxmoxSslUtil.IGNORE_SSL
            )

            log.debug("Removing network interface '${interfaceName}' from VM ${vmId} on node ${nodeName} with POST to: ${authConfig.apiUrl}${configPath}")
            log.debug("POST body: ${requestBody}")

            // 5. Make the API Call
            def results = ProxmoxApiUtil.callJsonApiWithRetry(client, 
                authConfig.apiUrl,
                configPath,
                null, // queryParams
                null, // body (using RequestOptions.body instead for POST)
                opts,
                'POST' // Using POST for config changes
            )

            log.debug("Remove network interface API response: ${results.toMap()}")

            // 6. Handle Response
            if (results.success) {
                log.info("Successfully initiated remove network interface operation for VM ${vmId}, interface ${interfaceName}. Task ID: ${results.data?.data}")
                return ServiceResponse.success("Network interface ${interfaceName} removal initiated successfully. Task ID: ${results.data?.data}", [taskId: results.data?.data])
            } else {
                String errorMessage = results.msg ?: results.content ?: "Unknown error"
                log.error("Failed to remove network interface ${interfaceName} from VM ${vmId}: ${errorMessage}")
                return ServiceResponse.error("Failed to remove network interface ${interfaceName}: ${errorMessage}")
            }

        } catch (e) {
            log.error("Error removing network interface ${interfaceName} from VM ${vmId}: ${e.message}", e)
            return ServiceResponse.error("Error removing network interface ${interfaceName} from VM ${vmId}: ${e.message}")
        }
    }

    static ServiceResponse updateVMNetworkInterface(HttpApiClient client, Map authConfig, String nodeName, String vmId, String interfaceName, String bridgeName, String model, String vlanTag, Boolean firewallEnabled) {
        log.debug("updateVMNetworkInterface: vmId=${vmId}, nodeName=${nodeName}, interfaceName=${interfaceName}, bridgeName=${bridgeName}, model=${model}, vlanTag=${vlanTag}, firewallEnabled=${firewallEnabled}")

        try {
            // 1. Authentication
            def tokenCfgResponse = getApiV2Token(authConfig)
            if (!tokenCfgResponse.success) {
                return ServiceResponse.error("Failed to get API token for Proxmox in updateVMNetworkInterface: ${tokenCfgResponse.msg}")
            }
            def tokenCfg = tokenCfgResponse.data

            // 2. Construct API Path
            String configPath = "${authConfig.v2basePath}/nodes/${nodeName}/qemu/${vmId}/config"

            // 3. Construct Network Configuration String (similar to addVMNetworkInterface)
            StringBuilder nicConfig = new StringBuilder()
            // Proxmox expects model first for netX if MAC is not provided, or model=MAC.
            // For updates, typically MAC is already set. We just provide the new configuration.
            // If bridge is not specified, Proxmox usually keeps the existing one if only other params are changed.
            // However, to be explicit and ensure the update is applied as intended, we specify all relevant parts.
            nicConfig.append("model=${model},bridge=${bridgeName}") // Explicitly set model and bridge

            if (vlanTag != null && !vlanTag.trim().isEmpty()) {
                nicConfig.append(",tag=${vlanTag.trim()}")
            }
            // For firewall, Proxmox expects 'firewall=1' or 'firewall=0'.
            // If firewallEnabled is null, we might not want to change the existing setting.
            // However, the method signature implies we are setting it.
            // If firewallEnabled is true, append firewall=1. If false, Proxmox might expect firewall=0 or for the param to be absent to disable.
            // Let's assume Proxmox handles "firewall=0" to disable. If not, this might need adjustment.
            // The pvesh set command for firewall seems to be `qm set 100 -net0 virtio,bridge=vmbr0,firewall=1` or `=0`
            if (firewallEnabled != null) {
                 nicConfig.append(",firewall=" + (firewallEnabled ? "1" : "0"))
            }
            
            String nicConfigString = nicConfig.toString()

            // 4. Create Request Body
            // The body key is the interface name (e.g., net0), and value is the config string.
            def requestBody = [
                (interfaceName): nicConfigString
            ]

            // 5. Prepare RequestOptions
            def opts = new HttpApiClient.RequestOptions(
                headers: [
                    'Content-Type'       : 'application/json',
                    'Cookie'             : "PVEAuthCookie=${tokenCfg.token}",
                    'CSRFPreventionToken': tokenCfg.csrfToken
                ],
                body: requestBody,
                contentType: ContentType.APPLICATION_JSON,
                ignoreSSL: ProxmoxSslUtil.IGNORE_SSL
            )

            log.debug("Updating network interface '${interfaceName}' on VM ${vmId} on node ${nodeName} with POST to: ${authConfig.apiUrl}${configPath}")
            log.debug("POST body: ${requestBody}")

            // 6. Make the API Call
            def results = ProxmoxApiUtil.callJsonApiWithRetry(client, 
                authConfig.apiUrl,
                configPath,
                null, // queryParams
                null, // body (using RequestOptions.body instead for POST)
                opts,
                'POST' // Using POST for config changes
            )

            log.debug("Update network interface API response: ${results.toMap()}")

            // 7. Handle Response
            if (results.success) {
                log.info("Successfully initiated update for network interface ${interfaceName} on VM ${vmId}. Task ID: ${results.data?.data}")
                return ServiceResponse.success("Network interface ${interfaceName} update initiated successfully. Task ID: ${results.data?.data}", [taskId: results.data?.data])
            } else {
                String errorMessage = results.msg ?: results.content ?: "Unknown error"
                log.error("Failed to update network interface ${interfaceName} on VM ${vmId}: ${errorMessage}")
                return ServiceResponse.error("Failed to update network interface ${interfaceName}: ${errorMessage}")
            }

        } catch (e) {
            log.error("Error updating network interface ${interfaceName} on VM ${vmId}: ${e.message}", e)
            return ServiceResponse.error("Error updating network interface ${interfaceName} on VM ${vmId}: ${e.message}")
        }
    }
}
