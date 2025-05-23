package com.morpheusdata.proxmox.ve

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.backup.AbstractBackupTypeProvider
import com.morpheusdata.core.backup.BackupExecutionProvider
import com.morpheusdata.core.backup.BackupRestoreProvider
import com.morpheusdata.model.BackupProvider as BackupProviderModel
import com.morpheusdata.model.OptionType
import com.morpheusdata.response.ServiceResponse
import groovy.util.logging.Slf4j
import java.util.ArrayList
import java.util.Collection

@Slf4j
class ProxmoxBackupTypeProvider extends AbstractBackupTypeProvider {

    ProxmoxBackupExecutionProvider executionProvider
    ProxmoxBackupRestoreProvider restoreProvider
    MorpheusContext morpheusContext
    ProxmoxVePlugin plugin

    ProxmoxBackupTypeProvider(ProxmoxVePlugin plugin, MorpheusContext morpheusContext) {
        super(plugin, morpheusContext)
        this.plugin = plugin
        this.morpheusContext = morpheusContext
    }

    @Override
    String getCode() {
        return "proxmoxSnapshot"
    }

    @Override
    String getName() {
        return "Proxmox VM Snapshot"
    }
    
    @Override
    String getContainerType() {
        return "single" // Typically for VM snapshots
    }
    
    @Override
    Boolean getCopyToStore() {
        return false // Proxmox snapshots are typically stored on the same storage as the VM
    }

    @Override
    Boolean getDownloadEnabled() {
        return false // Direct download of Proxmox snapshots is not a standard feature via this mechanism
    }

    @Override
    Boolean getRestoreExistingEnabled() {
        return true // Snapshots can be rolled back to the existing VM
    }

    @Override
    Boolean getRestoreNewEnabled() {
        return false // Restoring a snapshot to a new VM (clone from snapshot) might be possible but not implemented initially
    }

    @Override
    String getRestoreType() {
        return "offline" // Snapshot rollback usually requires VM to be offline or reboots
    }

    @Override
    String getRestoreNewMode() {
        return null // Not applicable as getRestoreNewEnabled is false
    }
    
    @Override
    Boolean getHasCopyToStore() {
        return false
    }

    @Override
    Collection<OptionType> getOptionTypes() {
        return new ArrayList<OptionType>() // No specific options for snapshot creation initially
    }
    
    @Override
    ProxmoxBackupExecutionProvider getExecutionProvider() {
        if(this.executionProvider == null) {
            this.executionProvider = new ProxmoxBackupExecutionProvider(plugin, morpheusContext)
        }
        return this.executionProvider
    }

    @Override
    ProxmoxBackupRestoreProvider getRestoreProvider() {
        if(this.restoreProvider == null) {
            this.restoreProvider = new ProxmoxBackupRestoreProvider(plugin, morpheusContext)
        }
        return this.restoreProvider
    }

    @Override
    ServiceResponse refresh(Map authConfig, BackupProviderModel backupProviderModel) {
        log.debug("ProxmoxBackupTypeProvider.refresh called for backupProviderModel: ${backupProviderModel?.name}")
        // TODO: Implement if any specific refresh logic is needed from Proxmox for this backup type
        return ServiceResponse.success()
    }
    
    @Override
    ServiceResponse clean(BackupProviderModel backupProviderModel, Map opts) {
        log.debug("ProxmoxBackupTypeProvider.clean called for backupProviderModel: ${backupProviderModel?.name}")
        // TODO: Implement if any specific cleanup logic is needed when the backup provider integration is removed
        return ServiceResponse.success()
    }
}
