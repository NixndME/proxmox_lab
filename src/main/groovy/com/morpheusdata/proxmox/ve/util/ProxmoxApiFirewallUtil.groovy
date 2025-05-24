package com.morpheusdata.proxmox.ve.util

import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.response.ServiceResponse
import groovy.util.logging.Slf4j
import org.apache.http.entity.ContentType

@Slf4j
class ProxmoxApiFirewallUtil {
    static {
        ProxmoxSslUtil.configureSslContextIfNeeded()
    }

    private static ServiceResponse getApiV2Token(Map authConfig) {
        def path = "access/ticket"
        log.debug("ProxmoxApiFirewallUtil.getApiV2Token: path: ${path} for apiUrl: ${authConfig.apiUrl}")
        HttpApiClient client = new HttpApiClient()
        def rtn = new ServiceResponse(success:false)
        try {
            if(!authConfig.username || !authConfig.password) {
                rtn.msg = "Username or password missing in authConfig for getApiV2Token."
                log.error(rtn.msg)
                return rtn
            }
            def encUid = URLEncoder.encode((String)authConfig.username, 'UTF-8')
            def encPwd = URLEncoder.encode((String)authConfig.password, 'UTF-8')
            def bodyStr = "username=" + "${encUid}" + "&password=${encPwd}"
            HttpApiClient.RequestOptions opts = new HttpApiClient.RequestOptions(
                headers:['Content-Type':'application/x-www-form-urlencoded'],
                body: bodyStr,
                contentType: ContentType.APPLICATION_FORM_URLENCODED,
                ignoreSSL: ProxmoxSslUtil.IGNORE_SSL
            )
            def results = ProxmoxApiUtil.callJsonApiWithRetry(client, authConfig.apiUrl, "${authConfig.v2basePath}/${path}", null, null, opts, 'POST')
            log.debug("getApiV2Token API response raw content: ${results.content}")
            if(results?.success && results.data?.data) {
                rtn.success = true
                def tokenData = results.data.data
                rtn.data = [csrfToken: tokenData.CSRFPreventionToken, token: tokenData.ticket]
            } else {
                rtn.success = false
                rtn.msg = results.msg ?: results.content ?: "Failed to retrieve Proxmox API token. ErrorCode: ${results.errorCode ?: 'N/A'}"
                log.error("Error retrieving Proxmox API token for ${authConfig.username}@${authConfig.apiUrl}: ${rtn.msg}")
            }
        } catch(e) {
            log.error("Exception in getApiV2Token for ${authConfig.username}@${authConfig.apiUrl}: ${e.message}", e)
            rtn.success = false
            rtn.msg = "Exception retrieving Proxmox API token: ${e.message}"
        } finally {
            client?.shutdownClient()
        }
        return rtn
    }

    static ServiceResponse listVmFirewallRules(HttpApiClient client, Map authConfig, String nodeId, String vmId) {
        log.debug("listVmFirewallRules node=${nodeId} vmId=${vmId}")
        def rtn = new ServiceResponse(success:false)
        try {
            def tokenResp = getApiV2Token(authConfig)
            if(!tokenResp.success) return tokenResp
            def tokenCfg = tokenResp.data
            def opts = new HttpApiClient.RequestOptions(
                headers:[
                    'Cookie':"PVEAuthCookie=${tokenCfg.token}",
                    'CSRFPreventionToken': tokenCfg.csrfToken
                ],
                contentType: ContentType.APPLICATION_JSON,
                ignoreSSL: ProxmoxSslUtil.IGNORE_SSL
            )
            String path = "${authConfig.v2basePath}/nodes/${nodeId}/qemu/${vmId}/firewall/rules"
            def results = ProxmoxApiUtil.callJsonApiWithRetry(client, authConfig.apiUrl, path, null, null, opts, 'GET')
            if(results?.success) {
                rtn.success = true
                rtn.data = results.data?.data
            } else {
                rtn = ProxmoxApiUtil.validateApiResponse(results, "Failed to list firewall rules for VM ${vmId}")
            }
        } catch(e) {
            log.error("Error listing firewall rules for VM ${vmId}: ${e.message}", e)
            rtn.success = false
            rtn.msg = "Error listing firewall rules: ${e.message}"
        }
        return rtn
    }

    static ServiceResponse createVmFirewallRule(HttpApiClient client, Map authConfig, String nodeId, String vmId, Map ruleConfig) {
        log.debug("createVmFirewallRule node=${nodeId} vmId=${vmId} rule=${ruleConfig}")
        def rtn = new ServiceResponse(success:false)
        try {
            def tokenResp = getApiV2Token(authConfig)
            if(!tokenResp.success) return tokenResp
            def tokenCfg = tokenResp.data
            def opts = new HttpApiClient.RequestOptions(
                headers:[
                    'Content-Type':'application/json',
                    'Cookie':"PVEAuthCookie=${tokenCfg.token}",
                    'CSRFPreventionToken': tokenCfg.csrfToken
                ],
                body: ruleConfig,
                contentType: ContentType.APPLICATION_JSON,
                ignoreSSL: ProxmoxSslUtil.IGNORE_SSL
            )
            String path = "${authConfig.v2basePath}/nodes/${nodeId}/qemu/${vmId}/firewall/rules"
            def results = ProxmoxApiUtil.callJsonApiWithRetry(client, authConfig.apiUrl, path, null, null, opts, 'POST')
            if(results?.success) {
                rtn.success = true
                rtn.data = results.data
            } else {
                rtn = ProxmoxApiUtil.validateApiResponse(results, "Failed to create firewall rule on VM ${vmId}")
            }
        } catch(e) {
            log.error("Error creating firewall rule on VM ${vmId}: ${e.message}", e)
            rtn.success = false
            rtn.msg = "Error creating firewall rule: ${e.message}"
        }
        return rtn
    }

    static ServiceResponse deleteVmFirewallRule(HttpApiClient client, Map authConfig, String nodeId, String vmId, Integer rulePos) {
        log.debug("deleteVmFirewallRule node=${nodeId} vmId=${vmId} pos=${rulePos}")
        def rtn = new ServiceResponse(success:false)
        try {
            def tokenResp = getApiV2Token(authConfig)
            if(!tokenResp.success) return tokenResp
            def tokenCfg = tokenResp.data
            def opts = new HttpApiClient.RequestOptions(
                headers:[
                    'Content-Type':'application/json',
                    'Cookie':"PVEAuthCookie=${tokenCfg.token}",
                    'CSRFPreventionToken': tokenCfg.csrfToken
                ],
                contentType: ContentType.APPLICATION_JSON,
                ignoreSSL: ProxmoxSslUtil.IGNORE_SSL
            )
            String path = "${authConfig.v2basePath}/nodes/${nodeId}/qemu/${vmId}/firewall/rules/${rulePos}"
            def results = ProxmoxApiUtil.callJsonApiWithRetry(client, authConfig.apiUrl, path, null, null, opts, 'DELETE')
            if(results?.success) {
                rtn.success = true
            } else {
                rtn = ProxmoxApiUtil.validateApiResponse(results, "Failed to delete firewall rule ${rulePos} on VM ${vmId}")
            }
        } catch(e) {
            log.error("Error deleting firewall rule on VM ${vmId}: ${e.message}", e)
            rtn.success = false
            rtn.msg = "Error deleting firewall rule: ${e.message}"
        }
        return rtn
    }
}
