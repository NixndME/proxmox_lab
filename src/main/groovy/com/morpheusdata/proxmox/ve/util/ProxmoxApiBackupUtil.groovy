package com.morpheusdata.proxmox.ve.util

import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.response.ServiceResponse
import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j
import org.apache.http.entity.ContentType
import com.morpheusdata.proxmox.ve.util.ProxmoxSslUtil

import java.net.URLEncoder

@Slf4j
class ProxmoxApiBackupUtil {

    static {
        ProxmoxSslUtil.configureSslContextIfNeeded()
    }

    // Method adapted from ProxmoxApiComputeUtil.groovy
    private static ServiceResponse getApiV2Token(Map authConfig) {
        def path = "access/ticket"
        log.debug("ProxmoxApiBackupUtil.getApiV2Token: path: \${path} for apiUrl: \${authConfig.apiUrl}")
        HttpApiClient client = new HttpApiClient() // Local client for token retrieval
        def rtn = new ServiceResponse(success: false)
        try {
            def encUid = URLEncoder.encode((String) authConfig.username, "UTF-8")
            def encPwd = URLEncoder.encode((String) authConfig.password, "UTF-8")
            def bodyStr = "username=" + "\${encUid}" + "&password=\${encPwd}"

            HttpApiClient.RequestOptions opts = new HttpApiClient.RequestOptions(
                    headers: ['Content-Type':'application/x-www-form-urlencoded'],
                    body: bodyStr,
                    contentType: ContentType.APPLICATION_FORM_URLENCODED,
                    ignoreSSL: ProxmoxSslUtil.IGNORE_SSL // TODO: Make this configurable based on cloud settings if possible
            )
            def results = client.callJsonApi(authConfig.apiUrl,"${authConfig.v2basePath}/\${path}", opts, 'POST')

            log.debug("getApiV2Token API request results: \${results.toMap()}")
            if(results?.success && !results?.hasErrors() && results.data?.data) {
                rtn.success = true
                def tokenData = results.data.data
                rtn.data = [csrfToken: tokenData.CSRFPreventionToken, token: tokenData.ticket]
            } else {
                rtn.success = false
                rtn.msg = "Error retrieving token: \${results.data ?: results.content}"
                log.error("Error retrieving token: \${rtn.msg}")
            }
        } catch(e) {
            log.error "Error in getApiV2Token: \${e.message}", e
            rtn.msg = "Error in getApiV2Token: \${e.message}"
            rtn.success = false
        } finally {
            client.shutdownClient() // Shutdown the local client used for token retrieval
        }
        return rtn
    }

    static ServiceResponse createSnapshot(HttpApiClient client, Map authConfig, String nodeId, String vmId, String snapshotName, String description) {
        log.debug("ProxmoxApiBackupUtil.createSnapshot: nodeId=\${nodeId}, vmId=\${vmId}, snapshotName='\${snapshotName}', description='\${description}'")
        ServiceResponse tokenResponse = getApiV2Token(authConfig)
        if (!tokenResponse.success) {
            return tokenResponse // Return error if token retrieval failed
        }
        def tokenCfg = tokenResponse.data
        def rtn = new ServiceResponse(success: false)

        try {
            def path = "nodes/\${nodeId}/qemu/\${vmId}/snapshot"
            def bodyPayload = [snapname: snapshotName]
            if (description != null && !description.isEmpty()) {
                bodyPayload.description = description
            }
            // Proxmox API for snapshot creation might require 'vmstatestore' parameter if the VM is running and live snapshot is desired.
            // For simplicity, assuming default behavior or that VM is offline if needed.
            // Add 'vmstatestore': 1 for live snapshots if required and supported.
            // bodyPayload.vmstatestore = 1 // Example for live snapshot, check Proxmox docs
            // TODO: Investigate and confirm Proxmox API requirements for 'vmstatestore' (live snapshots) and consider making it a user option.

            HttpApiClient.RequestOptions opts = new HttpApiClient.RequestOptions(
                headers: [
                    'Cookie': "PVEAuthCookie=\${tokenCfg.token}",
                    'CSRFPreventionToken': tokenCfg.csrfToken
                ],
                body: bodyPayload,
                contentType: ContentType.APPLICATION_JSON, // TODO: Verify Content-Type; Proxmox might expect x-www-form-urlencoded for this operation.
                ignoreSSL: ProxmoxSslUtil.IGNORE_SSL // TODO: Make this configurable
            )

            log.info("Creating Proxmox snapshot. API POST to \${authConfig.apiUrl}\${authConfig.v2basePath}/\${path} with body: \${bodyPayload}")
            def results = client.callJsonApi(authConfig.apiUrl, "\${authConfig.v2basePath}/\${path}", null, null, opts, 'POST')
            log.debug("Create snapshot API response: \${results.toMap()}")

            if (results?.success && results.data?.data) {
                rtn.success = true
                rtn.data = results.data // Usually contains the task ID
                rtn.msg = "Snapshot creation initiated successfully. Task ID: \${results.data.data}"
            } else {
                rtn.success = false
                rtn.msg = "Failed to create snapshot: \${results.msg ?: results.content}"
                log.error(rtn.msg)
            }
        } catch (Exception e) {
            log.error("Exception during createSnapshot: \${e.message}", e)
            rtn.success = false
            rtn.msg = "Exception creating snapshot: \${e.message}"
        }
        return rtn
    }

    static ServiceResponse deleteSnapshot(HttpApiClient client, Map authConfig, String nodeId, String vmId, String snapshotName) {
        log.debug("ProxmoxApiBackupUtil.deleteSnapshot: nodeId=\${nodeId}, vmId=\${vmId}, snapshotName='\${snapshotName}'")
        ServiceResponse tokenResponse = getApiV2Token(authConfig)
        if (!tokenResponse.success) {
            return tokenResponse
        }
        def tokenCfg = tokenResponse.data
        def rtn = new ServiceResponse(success: false)

        try {
            def path = "nodes/\${nodeId}/qemu/\${vmId}/snapshot/\${snapshotName}"
            HttpApiClient.RequestOptions opts = new HttpApiClient.RequestOptions(
                headers: [
                    'Cookie': "PVEAuthCookie=\${tokenCfg.token}",
                    'CSRFPreventionToken': tokenCfg.csrfToken
                ],
                ignoreSSL: ProxmoxSslUtil.IGNORE_SSL // TODO: Make this configurable
            )

            log.info("Deleting Proxmox snapshot. API DELETE to \${authConfig.apiUrl}\${authConfig.v2basePath}/\${path}")
            def results = client.callJsonApi(authConfig.apiUrl, "\${authConfig.v2basePath}/\${path}", null, null, opts, 'DELETE')
            log.debug("Delete snapshot API response: \${results.toMap()}")

            if (results?.success && results.data?.data) {
                rtn.success = true
                rtn.data = results.data // Usually contains the task ID
                rtn.msg = "Snapshot deletion initiated successfully. Task ID: \${results.data.data}"
            } else {
                // Proxmox might return 200 OK with null data on successful deletion, or if snapshot doesn't exist.
                // Or it might return an error if it fails. This needs testing.
                // If result.success is true but no data, assume it worked.
                if(results?.success) {
                     rtn.success = true
                     rtn.msg = "Snapshot deletion request processed. Proxmox response indicated success."
                } else {
                    rtn.success = false
                    rtn.msg = "Failed to delete snapshot: \${results.msg ?: results.content}"
                    log.error(rtn.msg)
                }
            }
        } catch (Exception e) {
            log.error("Exception during deleteSnapshot: \${e.message}", e)
            rtn.success = false
            rtn.msg = "Exception deleting snapshot: \${e.message}"
        }
        return rtn
    }
    
    static ServiceResponse rollbackSnapshot(HttpApiClient client, Map authConfig, String nodeId, String vmId, String snapshotName) {
        log.debug("ProxmoxApiBackupUtil.rollbackSnapshot: nodeId=\${nodeId}, vmId=\${vmId}, snapshotName='\${snapshotName}'")
        ServiceResponse tokenResponse = getApiV2Token(authConfig)
        if (!tokenResponse.success) {
            return tokenResponse
        }
        def tokenCfg = tokenResponse.data
        def rtn = new ServiceResponse(success: false)

        try {
            // Proxmox API to rollback a snapshot is a POST to the snapshot name itself
            def path = "nodes/\${nodeId}/qemu/\${vmId}/snapshot/\${snapshotName}/rollback" 
            // The body for rollback is typically empty or might take a parameter like 'start': 1 to start the VM after rollback.
            // For now, assuming an empty body.
            def bodyPayload = [:] 

            HttpApiClient.RequestOptions opts = new HttpApiClient.RequestOptions(
                headers: [
                    'Cookie': "PVEAuthCookie=\${tokenCfg.token}",
                    'CSRFPreventionToken': tokenCfg.csrfToken
                ],
                body: bodyPayload, 
                contentType: ContentType.APPLICATION_JSON, // TODO: Verify Content-Type; Proxmox might expect x-www-form-urlencoded for this operation.
                ignoreSSL: ProxmoxSslUtil.IGNORE_SSL // TODO: Make this configurable
            )

            log.info("Rolling back Proxmox snapshot. API POST to \${authConfig.apiUrl}\${authConfig.v2basePath}/\${path}")
            def results = client.callJsonApi(authConfig.apiUrl, "\${authConfig.v2basePath}/\${path}", null, null, opts, 'POST')
            log.debug("Rollback snapshot API response: \${results.toMap()}")

            if (results?.success && results.data?.data) {
                rtn.success = true
                rtn.data = results.data // Usually contains the task ID
                rtn.msg = "Snapshot rollback initiated successfully. Task ID: \${results.data.data}"
            } else {
                rtn.success = false
                rtn.msg = "Failed to rollback snapshot: \${results.msg ?: results.content}"
                log.error(rtn.msg)
            }
        } catch (Exception e) {
            log.error("Exception during rollbackSnapshot: \${e.message}", e)
            rtn.success = false
            rtn.msg = "Exception rolling back snapshot: \${e.message}"
        }
        return rtn
    }

    // Optional: Methods for getSnapshot and listSnapshots can be added here if needed later.
    // static ServiceResponse getSnapshot(HttpApiClient client, Map authConfig, String nodeId, String vmId, String snapshotName) { ... }
    // static ServiceResponse listSnapshots(HttpApiClient client, Map authConfig, String nodeId, String vmId) { ... }
}
