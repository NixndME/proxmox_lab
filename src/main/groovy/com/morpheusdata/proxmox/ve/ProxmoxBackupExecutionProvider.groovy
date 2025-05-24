package com.morpheusdata.proxmox.ve

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.backup.BackupExecutionProvider
import com.morpheusdata.core.backup.response.BackupExecutionResponse
import com.morpheusdata.core.util.DateUtility
import com.morpheusdata.model.Backup
import com.morpheusdata.model.BackupResult
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.model.Workload
import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.proxmox.ve.util.ProxmoxApiBackupUtil
import com.morpheusdata.proxmox.ve.util.ProxmoxRetryUtil
import com.morpheusdata.core.util.HttpApiClient
import groovy.util.logging.Slf4j
import java.util.Date

@Slf4j
class ProxmoxBackupExecutionProvider implements BackupExecutionProvider {

    ProxmoxVePlugin plugin
    MorpheusContext morpheusContext

    ProxmoxBackupExecutionProvider(ProxmoxVePlugin plugin, MorpheusContext morpheusContext) {
        this.plugin = plugin
        this.morpheusContext = morpheusContext
    }
    
    MorpheusContext getMorpheus() {
        return morpheusContext
    }

    @Override
    ServiceResponse configureBackup(Backup backup, Map config, Map opts) {
        log.debug("ProxmoxBackupExecutionProvider.configureBackup for backup: ${backup.name}, config: ${config}, opts: ${opts}")
        return ServiceResponse.success(backup)
    }

    @Override
    ServiceResponse validateBackup(Backup backup, Map config, Map opts) {
        log.debug("ProxmoxBackupExecutionProvider.validateBackup for backup: ${backup.name}, config: ${config}, opts: ${opts}")
        return ServiceResponse.success(backup)
    }

    @Override
    ServiceResponse createBackup(Backup backup, Map opts) {
        log.debug("ProxmoxBackupExecutionProvider.createBackup for backup: ${backup.name}, opts: ${opts}")
        return ServiceResponse.success()
    }

    @Override
    ServiceResponse deleteBackup(Backup backup, Map opts) {
        log.debug("ProxmoxBackupExecutionProvider.deleteBackup for backup: ${backup.name}, opts: ${opts}")
        return ServiceResponse.success()
    }

    @Override
    ServiceResponse deleteBackupResult(BackupResult backupResult, Map opts) {
        log.debug("ProxmoxBackupExecutionProvider.deleteBackupResult for backupResult id: ${backupResult.id}, opts: ${opts}")
        ServiceResponse rtn = ServiceResponse.prepare()
        HttpApiClient client = new HttpApiClient()
        try {
            def config = backupResult.getConfigMap() ?: [:]
            Cloud cloud = morpheusContext.services.cloud.get(backupResult.backup.cloud.id).blockingGet()
            ComputeServer server = null
            if(backupResult.server?.id) {
                server = morpheusContext.services.computeServer.get(backupResult.server.id).blockingGet()
            } else if(backupResult.containerId) {
                Workload container = morpheusContext.services.workload.get(backupResult.containerId).blockingGet()
                if(container?.serverId) {
                    server = morpheusContext.services.computeServer.get(container.serverId).blockingGet()
                }
            }

            def snapshotName = backupResult.snapshotId ?: config.snapshotId

            if (!cloud) {
                 rtn.setSuccess(false)
                 rtn.setMsg("Cloud not found for backup result ${backupResult.id}")
                 log.error(rtn.msg)
                 return rtn
            }
            if (!server) {
                rtn.setSuccess(false)
                rtn.setMsg("Server not found for backup result ${backupResult.id}")
                log.error(rtn.msg)
                return rtn
            }
            if (!snapshotName) {
                rtn.setSuccess(false)
                rtn.setMsg("Snapshot name/ID not found in backup result ${backupResult.id}")
                log.error(rtn.msg)
                return rtn
            }

            def authConfig = plugin.getAuthConfig(cloud)
            // TODO: Review and enhance proxmoxNode determination for robustness across various server/setup configurations.
            String proxmoxNode = server.parentServer?.name ?: server.parentServer?.externalId
            if(!proxmoxNode && server.parentServerId) {
                ComputeServer parentSrv = morpheusContext.services.computeServer.get(server.parentServerId).blockingGet()
                proxmoxNode = parentSrv?.name ?: parentSrv?.externalId
            }
            if(!proxmoxNode) {
                 proxmoxNode = server.getConfigProperty('proxmoxNode')
            }
             if(!proxmoxNode) { // Fallback to config map on backup result itself
                proxmoxNode = config.proxmoxNode
            }


            if (!proxmoxNode) {
                rtn.setSuccess(false)
                rtn.setMsg("Proxmox node not found for server ${server.name} (${server.id}) for backup result ${backupResult.id}")
                log.error(rtn.msg)
                return rtn
            }

            log.info("Attempting to delete Proxmox snapshot: '${snapshotName}' for VM ID: ${server.externalId} on node: ${proxmoxNode}")
            ServiceResponse deleteSnapResponse = ProxmoxRetryUtil.executeWithRetry({
                ProxmoxApiBackupUtil.deleteSnapshot(client, authConfig, proxmoxNode, server.externalId, snapshotName)
            }, "delete snapshot '${snapshotName}'")

            if (deleteSnapResponse.success) {
                log.info("Successfully deleted snapshot '${snapshotName}' from Proxmox for backup result ${backupResult.id}")
                rtn.setSuccess(true)
            } else {
                log.error("Failed to delete snapshot '${snapshotName}' from Proxmox: ${deleteSnapResponse.msg}")
                rtn.setSuccess(false)
                rtn.setMsg("Failed to delete snapshot from Proxmox: ${deleteSnapResponse.msg}")
            }
        } catch (Exception e) {
            log.error("Error in deleteBackupResult for Proxmox: ${e.message}", e)
            rtn.setSuccess(false)
            rtn.setMsg("Error deleting Proxmox snapshot: ${e.message}")
        } finally {
            client?.shutdownClient()
        }
        return rtn
    }

    @Override
    ServiceResponse prepareExecuteBackup(Backup backup, Map opts) {
        log.debug("ProxmoxBackupExecutionProvider.prepareExecuteBackup for backup: ${backup.name}, opts: ${opts}")
        return ServiceResponse.success()
    }

    @Override
    ServiceResponse prepareBackupResult(BackupResult backupResultModel, Map opts) {
        log.debug("ProxmoxBackupExecutionProvider.prepareBackupResult for backupResult id: ${backupResultModel.id}, opts: ${opts}")
        return ServiceResponse.success()
    }

    @Override
    ServiceResponse<BackupExecutionResponse> executeBackup(Backup backup, BackupResult backupResult, Map executionConfig, Cloud cloud, ComputeServer server, Map opts) {
        log.debug("ProxmoxBackupExecutionProvider.executeBackup for backup: ${backup.name}, server: ${server.name}, executionConfig: ${executionConfig}, opts: ${opts}")
        ServiceResponse<BackupExecutionResponse> rtn = ServiceResponse.prepare(new BackupExecutionResponse(backupResult))
        HttpApiClient client = new HttpApiClient()
        try {
            backupResult.status = BackupResult.Status.IN_PROGRESS
            morpheusContext.async.backup.saveResult(backupResult).blockingGet()

            def snapshotName = "morpheus-${backup.id}-${System.currentTimeMillis()}"
            def description = "Morpheus Backup: ${backup.name} for server ${server.name} (${server.id}) - ${new Date().toString()}"
            
            def authConfig = plugin.getAuthConfig(cloud)
            // TODO: Review and enhance proxmoxNode determination for robustness across various server/setup configurations.
            String proxmoxNode = server.parentServer?.name ?: server.parentServer?.externalId
             if(!proxmoxNode && server.parentServerId) {
                ComputeServer parentSrv = morpheusContext.services.computeServer.get(server.parentServerId).blockingGet()
                proxmoxNode = parentSrv?.name ?: parentSrv?.externalId
            }
            if(!proxmoxNode) {
                 proxmoxNode = server.getConfigProperty('proxmoxNode')
            }

            if (!proxmoxNode) {
                throw new IllegalStateException("Proxmox node not found for server ${server.name} (${server.id})")
            }
            
            log.info("Attempting to create Proxmox snapshot: '${snapshotName}' for VM ID: ${server.externalId} on node: ${proxmoxNode}. Description: '${description}'")
            ServiceResponse createSnapResponse = ProxmoxRetryUtil.executeWithRetry({
                ProxmoxApiBackupUtil.createSnapshot(client, authConfig, proxmoxNode, server.externalId, snapshotName, description)
            }, "create snapshot '${snapshotName}'")

            if (createSnapResponse.success) {
                log.info("Successfully created snapshot '${snapshotName}' in Proxmox. Task ID: ${createSnapResponse.data?.data}")
                backupResult.snapshotId = snapshotName 
                backupResult.externalId = createSnapResponse.data?.data // Proxmox task ID
                backupResult.status = BackupResult.Status.SUCCEEDED
                backupResult.setConfigProperty("proxmoxNode", proxmoxNode)
                backupResult.setConfigProperty("vmExternalId", server.externalId)
                backupResult.setConfigProperty("snapshotDescription", description)
            } else {
                log.error("Failed to create snapshot '${snapshotName}' in Proxmox: ${createSnapResponse.msg}")
                backupResult.status = BackupResult.Status.FAILED
                backupResult.errorOutput = createSnapResponse.msg?.encodeAsBase64()
            }
        } catch (Exception e) {
            log.error("Error in executeBackup for Proxmox: ${e.message}", e)
            backupResult.status = BackupResult.Status.FAILED
            backupResult.errorOutput = e.message?.encodeAsBase64()
            rtn.setSuccess(false)
            rtn.setMsg(e.message)
        } finally {
            backupResult.endDate = new Date()
            if (backupResult.startDate) {
                def start = backupResult.startDate
                def end = backupResult.endDate
                backupResult.durationMillis = end.time - start.time
            }
            morpheusContext.async.backup.saveResult(backupResult).blockingGet()
            rtn.data.backupResult = backupResult
            client?.shutdownClient()
        }
        rtn.setSuccess(backupResult.status == BackupResult.Status.SUCCEEDED)
        return rtn
    }

    @Override
    ServiceResponse<BackupExecutionResponse> refreshBackupResult(BackupResult backupResult) {
        log.debug("ProxmoxBackupExecutionProvider.refreshBackupResult for backupResult id: ${backupResult.id}")
        // TODO: Implement if Proxmox snapshot creation is asynchronous and needs status polling.
        return ServiceResponse.success(new BackupExecutionResponse(backupResult))
    }
    
    @Override
    ServiceResponse cancelBackup(BackupResult backupResultModel, Map opts) {
        log.debug("ProxmoxBackupExecutionProvider.cancelBackup for backupResult id: ${backupResultModel.id}, opts: ${opts}")
        return ServiceResponse.error("Snapshot cancellation not supported by Proxmox provider.")
    }

    @Override
    ServiceResponse extractBackup(BackupResult backupResultModel, Map opts) {
        log.debug("ProxmoxBackupExecutionProvider.extractBackup for backupResult id: ${backupResultModel.id}, opts: ${opts}")
        return ServiceResponse.error("Snapshot extraction not supported by Proxmox provider.")
    }
}
