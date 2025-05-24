package com.morpheusdata.proxmox.ve

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.backup.BackupRestoreProvider
import com.morpheusdata.core.backup.response.BackupRestoreResponse
import com.morpheusdata.model.Backup
import com.morpheusdata.model.BackupRestore
import com.morpheusdata.model.BackupResult
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.model.Instance
import com.morpheusdata.model.Workload
import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.proxmox.ve.util.ProxmoxApiBackupUtil
import com.morpheusdata.core.util.HttpApiClient
import groovy.util.logging.Slf4j
import java.util.Date

@Slf4j
class ProxmoxBackupRestoreProvider implements BackupRestoreProvider {

    ProxmoxVePlugin plugin
    MorpheusContext morpheusContext

    ProxmoxBackupRestoreProvider(ProxmoxVePlugin plugin, MorpheusContext morpheusContext) {
        this.plugin = plugin
        this.morpheusContext = morpheusContext
    }

    MorpheusContext getMorpheus() {
        return morpheusContext
    }

    // Legacy internal validation method
    ServiceResponse validateRestore(BackupResult backupResult, Instance instance, ComputeServer server, Map config, Map opts) {
        log.debug("ProxmoxBackupRestoreProvider.validateRestore for backupResult: ${backupResult.id}, instance: ${instance?.name}, server: ${server?.name}")
        if (!backupResult.snapshotId && !(backupResult.getConfigMap()?.snapshotId)) {
            return ServiceResponse.error("Snapshot ID not found in BackupResult, cannot restore.")
        }
        return ServiceResponse.success()
    }

    // Legacy internal preparation method
    ServiceResponse prepareRestore(BackupResult backupResult, Instance instance, ComputeServer server, Map config, Map opts) {
        log.debug("ProxmoxBackupRestoreProvider.prepareRestore for backupResult: ${backupResult.id}")
        return ServiceResponse.success()
    }

    // Legacy internal execution method
    ServiceResponse<BackupRestoreResponse> executeRestore(BackupResult backupResult, Instance instance, Workload workload, ComputeServer server, Map<String,Object> config, Map<String,Object> opts) {
        log.debug("ProxmoxBackupRestoreProvider.executeRestore for backupResult: ${backupResult.id} to server: ${server.name}")
        ServiceResponse<BackupRestoreResponse> rtn = ServiceResponse.prepare(new BackupRestoreResponse(backupResult))
        HttpApiClient client = new HttpApiClient()
        try {
            // TODO: Implement proper persistence of restore status, errors, and dates on BackupResult.
            // Example: backupResult.restoreStatus = ...; backupResult.restoreError = ...; backupResult.restoreEndDate = new Date(); morpheusContext.async.backup.saveResult(backupResult).blockingGet();
            // backupResult.status = BackupResult.Status.IN_PROGRESS // This is for backup status, not restore.

            def snapshotName = backupResult.snapshotId ?: backupResult.getConfigMap()?.snapshotId
            if (!snapshotName) {
                throw new IllegalStateException("Snapshot name/ID not found in backup result ${backupResult.id}")
            }

            def cloud = morpheusContext.services.cloud.get(server.cloud.id).blockingGet()
            def authConfig = plugin.getAuthConfig(cloud)
            
            // TODO: Review and enhance proxmoxNode determination for robustness across various server/setup configurations.
            String proxmoxNode = server.parentServer?.name ?: server.parentServer?.externalId
            if(!proxmoxNode && server.parentServerId) {
                ComputeServer parentSrv = morpheusContext.services.computeServer.get(server.parentServerId).blockingGet()
                proxmoxNode = parentSrv?.name ?: parentSrv?.externalId
            }
            if(!proxmoxNode) {
                 proxmoxNode = server.getConfigProperty('proxmoxNode') ?: backupResult.getConfigMap()?.proxmoxNode
            }
            
            if (!proxmoxNode) {
                throw new IllegalStateException("Proxmox node not found for server ${server.name} (${server.id})")
            }

            log.info("Attempting to roll back Proxmox snapshot: '${snapshotName}' for VM ID: ${server.externalId} on node: ${proxmoxNode}")
            ServiceResponse rollbackResponse = ProxmoxApiBackupUtil.rollbackSnapshot(client, authConfig, proxmoxNode, server.externalId, snapshotName)

            if (rollbackResponse.success) {
                log.info("Successfully initiated rollback of snapshot '${snapshotName}' in Proxmox. Task ID: ${rollbackResponse.data?.data}")
                // TODO: Verify Proxmox behavior: Does VM need to be manually started after rollback? If so, implement server start.
                // Example: plugin.getProvider(ProxmoxVeProvisionProvider.PROVISION_PROVIDER_CODE).startServer(server)
                // TODO: If rollback is asynchronous, store task ID (rollbackResponse.data.data) on backupResult for polling in refreshRestore.
            } else {
                log.error("Failed to rollback snapshot '${snapshotName}' in Proxmox: ${rollbackResponse.msg}")
                rtn.setSuccess(false) // Ensure overall response reflects failure
                rtn.setMsg(rollbackResponse.msg)
            }

        } catch (Exception e) {
            log.error("Error in executeRestore for Proxmox: ${e.message}", e)
            rtn.setSuccess(false)
            rtn.setMsg(e.message)
        } finally {
            // TODO: Ensure restore status (success/failure), error message, and duration are saved on backupResult (see TODO at start of try block).
            rtn.data.backupResult = backupResult
            client?.shutdownClient()
        }
         // if rtn success was not set to false explicitly, it defaults to true from prepare()
        if(rtn.msg != null && rtn.success) { // if there's a message but success is still true, it's likely an error
            rtn.setSuccess(false)
        } else if (rtn.msg == null && !rtn.success) { // if no message but success is false
             rtn.setMsg("Restore failed due to an unspecified error.")
        } else if (rtn.success && rtn.msg == null) {
             // If successful and no message was set, provide a generic success message
             rtn.setMsg("Restore operation completed successfully.")
        }
        return rtn
    }

    // Legacy internal refresh method
    ServiceResponse<BackupRestoreResponse> refreshRestore(BackupResult backupResult, Instance instance, ComputeServer server, Map opts) {
        log.debug("ProxmoxBackupRestoreProvider.refreshRestore for backupResult: ${backupResult.id}")
        // TODO: Implement if Proxmox snapshot rollback is asynchronous.
        return ServiceResponse.success(new BackupRestoreResponse(backupResult))
    }

    // Legacy internal failure handler
    ServiceResponse failRestore(BackupResult backupResult, Instance instance, ComputeServer server, Map opts) {
        log.debug("ProxmoxBackupRestoreProvider.failRestore for backupResult: ${backupResult.id}")
        // backupResult.restoreStatus = BackupResult.Status.FAILED // Example
        // morpheusContext.async.backup.saveResult(backupResult).blockingGet()
        return ServiceResponse.success()
    }

    // Legacy internal cleanup method
    ServiceResponse cleanupRestore(BackupResult backupResult, Instance instance, ComputeServer server, Map opts) {
        log.debug("ProxmoxBackupRestoreProvider.cleanupRestore for backupResult: ${backupResult.id}")
        return ServiceResponse.success()
    }

    /*
     * Methods required by the Morpheus BackupRestoreProvider interface
     * These wrap the legacy internal implementations above.
     */

    @Override
    ServiceResponse configureRestoreBackup(BackupResult backupResult, Map config, Map opts) {
        Instance instance
        ComputeServer server
        Workload workload
        if(backupResult.containerId) {
            workload = morpheusContext.services.workload.get(backupResult.containerId).blockingGet()
            if(workload?.serverId) {
                server = morpheusContext.services.computeServer.get(workload.serverId).blockingGet()
            }
            if(workload?.instanceId) {
                instance = morpheusContext.services.instance.get(workload.instanceId).blockingGet()
            }
        } else if(backupResult.server?.id) {
            server = morpheusContext.services.computeServer.get(backupResult.server.id).blockingGet()
        }
        return prepareRestore(backupResult, instance, server, config, opts)
    }

    @Override
    ServiceResponse validateRestoreBackup(BackupResult backupResult, Map opts) {
        Instance instance
        ComputeServer server
        if(backupResult.containerId) {
            Workload container = morpheusContext.services.workload.get(backupResult.containerId).blockingGet()
            if(container?.serverId) {
                server = morpheusContext.services.computeServer.get(container.serverId).blockingGet()
            }
            if(container?.instanceId) {
                instance = morpheusContext.services.instance.get(container.instanceId).blockingGet()
            }
        } else if(backupResult.server?.id) {
            server = morpheusContext.services.computeServer.get(backupResult.server.id).blockingGet()
        }
        return validateRestore(backupResult, instance, server, [:], opts)
    }

    @Override
    ServiceResponse<Map<String,Object>> getRestoreOptions(Backup backup, Map opts) {
        // No custom options for now
        return ServiceResponse.success([:])
    }

    @Override
    ServiceResponse<Map<String,Object>> getBackupRestoreInstanceConfig(BackupResult backupResult, Instance instance, Map config, Map opts) {
        // Return an empty config map wrapped in a ServiceResponse; override if provider needs specific values
        return ServiceResponse.success([:])
    }

    @Override
    ServiceResponse<BackupRestoreResponse> restoreBackup(BackupRestore backupRestore, BackupResult backupResult, Backup backup, Map opts) {
        Instance instance
        ComputeServer server
        Workload workload
        if(backupResult.containerId) {
            workload = morpheusContext.services.workload.get(backupResult.containerId).blockingGet()
            if(workload?.serverId) {
                server = morpheusContext.services.computeServer.get(workload.serverId).blockingGet()
            }
            if(workload?.instanceId) {
                instance = morpheusContext.services.instance.get(workload.instanceId).blockingGet()
            }
        } else if(backupResult.server?.id) {
            server = morpheusContext.services.computeServer.get(backupResult.server.id).blockingGet()
        }
        Map<String,Object> config = opts ?: [:]
        return executeRestore(backupResult, instance, workload, server, config, opts)
    }

    @Override
    ServiceResponse<BackupRestoreResponse> refreshBackupRestoreResult(BackupRestore backupRestore, BackupResult backupResult) {
        Instance instance
        ComputeServer server
        if(backupResult.containerId) {
            Workload container = morpheusContext.services.workload.get(backupResult.containerId).blockingGet()
            if(container?.serverId) {
                server = morpheusContext.services.computeServer.get(container.serverId).blockingGet()
            }
            if(container?.instanceId) {
                instance = morpheusContext.services.instance.get(container.instanceId).blockingGet()
            }
        } else if(backupResult.server?.id) {
            server = morpheusContext.services.computeServer.get(backupResult.server.id).blockingGet()
        }
        return refreshRestore(backupResult, instance, server, [:])
    }
}
