package com.morpheus.proxmox.ve.util

import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.response.ServiceResponse
import groovy.util.logging.Slf4j
import org.apache.http.entity.ContentType


@Slf4j
class ProxmoxComputeUtil {

    static final String API_BASE_PATH = "/api2/json"


    static cloneTemplate(HttpApiClient client, Map authConfig, String templateId, String name, String nodeId) {

        def rtn = new ServiceResponse(success: true)
        def nextId = callListApiV2(client, "cluster/nextid", authConfig).data
        log.info("Next VM Id is: $nextId")

        try {
            def tokenCfg = getApiV2Token(authConfig.username, authConfig.password, authConfig.apiUrl).data
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
                            name: name
                    ],
                    contentType: ContentType.APPLICATION_JSON,
                    ignoreSSL: true
            ]

            log.info("Cloning template $templateId to VM $name($nextId) on node $nodeId")
            log.info("Path is: $authConfig.apiUrl${authConfig.v2basePath}/nodes/$nodeId/qemu/$templateId/clone")
            def results = client.callJsonApi(
                    (String) authConfig.apiUrl,
                    "${authConfig.v2basePath}/nodes/$nodeId/qemu/$templateId/clone",
                    null, null,
                    new HttpApiClient.RequestOptions(opts),
                    'POST'
            )

            log.info("Cloning template $templateId to VM $nextId on node $nodeId")
            def resultData = results.content
            if(results?.success && !results?.hasErrors()) {
                rtn.success = true
                rtn.data = resultData
                opts.body = [vmid: nextId, node: nodeId]
                def startResults = client.callJsonApi(
                        (String) authConfig.apiUrl,
                        "${authConfig.v2basePath}/nodes/$nodeId/qemu/$nextId/status/start",
                        null, null,
                        new HttpApiClient.RequestOptions(opts),
                        'POST'
                )
            } else {
                rtn.msg = "Provisioning failed: $results.data $results $results.errorCode $results.content"
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

        ServiceResponse datastoreResults = callListApiV2(client, "storage", authConfig)
        List<Map> datastores = datastoreResults.data
        String queryNode = ""
        String randomNode = null
        for (ds in datastores) {
            if (ds.containsKey("nodes")) { //some pools don't belong to any node, but api path needs node for status details
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
        }
        datastoreResults.data = datastores
        return datastoreResults
    }


    static ServiceResponse listProxmoxNetworks(HttpApiClient client, Map authConfig) {
        log.debug("listProxmoxNetworks...")

        Collection<Map> networks = []
        ServiceResponse hosts = listProxmoxHypervisorHosts(client, authConfig)

        hosts.data.each {
            ServiceResponse hostNetworks = callListApiV2(client, "nodes/${it.node}/network", authConfig)
            hostNetworks.data.each { Map network ->
                if (network?.type == 'bridge') {
                    networks << (network)
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
        log.debug("API Util listVMs")
        def vms = []
        def qemuVMs = callListApiV2(client, "cluster/resources", authConfig)
        qemuVMs.data.each { Map vm ->
            if (vm?.template == 0 && vm?.type == "qemu") {
                def vmAgentInfo = callListApiV2(client, "nodes/$vm.node/qemu/$vm.vmid/agent/network-get-interfaces", authConfig)
                vm.ip = "0.0.0.0"
                if (vmAgentInfo.success) {
                    def results = vmAgentInfo.data?.result
                    results.each {
                        if (it."ip-address-type" == "ipv4" && it."ip-address" != "127.0.0.1" && vm.ip == "0.0.0.0") {
                            vm.ip = it."ip-address"
                        }
                    }
                }
                def vmCPUInfo = callListApiV2(client, "nodes/$vm.node/qemu/$vm.vmid/config", authConfig)
                vm.maxCores = (vmCPUInfo?.data?.data?.sockets?.toInteger() ?: 0) * (vmCPUInfo?.data?.data?.cores?.toInteger() ?: 0)
                vm.coresPerSocket = vmCPUInfo?.data?.data?.cores?.toInteger() ?: 0
                vms << vm
            }
        }
        return new ServiceResponse(success: true, data: vms)
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
        log.debug("callListApiV2: path: ${path}")

        def tokenCfg = getApiV2Token(authConfig.username, authConfig.password, authConfig.apiUrl).data
        def rtn = new ServiceResponse(success: false)
        try {
            rtn.data = []
            def opts = new HttpApiClient.RequestOptions(
                    headers: [
                        'Content-Type': 'application/json',
                        'Cookie': "PVEAuthCookie=$tokenCfg.token",
                        'CSRFPreventionToken': tokenCfg.csrfToken
                    ],
                    contentType: ContentType.APPLICATION_JSON,
                    ignoreSSL: true
            )
            def results = client.callJsonApi(authConfig.apiUrl, "${authConfig.v2basePath}/${path}", null, null, opts, 'GET')
            def resultData = results.toMap().data.data
            log.debug("callListApiV2 results: ${resultData}")
            if(results?.success && !results?.hasErrors()) {
                rtn.success = true
                rtn.data = resultData
            } else {
                if(!rtn.success) {
                    rtn.msg = results.data + results.errors
                    rtn.success = false
                }
            }
        } catch(e) {
            log.error "Error in callListApiV2: ${e}", e
            rtn.msg = "Error in callListApiV2: ${e}"
            rtn.success = false
        }
        return rtn
    }


    private static ServiceResponse getApiV2Token(String uid, String pwd, String baseUrl) {
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
                    ignoreSSL: true
            )
            def results = client.callJsonApi(baseUrl,"${API_BASE_PATH}/${path}", opts, 'POST')

            //log.debug("callListApiV2 results: ${results.toMap()}")
            if(results?.success && !results?.hasErrors()) {
                rtn.success = true
                def tokenData = results.data.data
                rtn.data = [csrfToken: tokenData.CSRFPreventionToken, token: tokenData.ticket]

                //log.info("CSRF: $csrfToken, Token: $token")
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
}